package com.avaje.ebeanservice.elastic;

import com.avaje.ebean.PagedList;
import com.avaje.ebean.PersistenceIOException;
import com.avaje.ebean.Query;
import com.avaje.ebean.QueryEachConsumer;
import com.avaje.ebean.plugin.BeanDocType;
import com.avaje.ebean.plugin.BeanType;
import com.avaje.ebean.plugin.Property;
import com.avaje.ebean.plugin.SpiServer;
import com.avaje.ebean.text.json.JsonBeanReader;
import com.avaje.ebean.text.json.JsonReadOptions;
import com.avaje.ebeaninternal.api.SpiQuery;
import com.avaje.ebeanservice.docstore.api.DocumentNotFoundException;
import com.avaje.ebeanservice.elastic.search.BeanSearchParser;
import com.avaje.ebeanservice.elastic.search.HitsPagedList;
import com.avaje.ebeanservice.elastic.search.RawSource;
import com.avaje.ebeanservice.elastic.search.RawSourceReader;
import com.avaje.ebeanservice.elastic.support.ElasticBatchUpdate;
import com.avaje.ebeanservice.elastic.support.IndexMessageResponse;
import com.avaje.ebeanservice.elastic.support.IndexMessageSender;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 *
 */
public class ElasticQueryService {

  private static final Logger logger = LoggerFactory.getLogger(ElasticDocumentStore.class);

  final SpiServer server;

  final JsonFactory jsonFactory;

  final IndexMessageSender messageSender;

  public ElasticQueryService(SpiServer server, JsonFactory jsonFactory, IndexMessageSender messageSender) {
    this.server = server;
    this.jsonFactory = jsonFactory;
    this.messageSender = messageSender;
  }

  public <T> PagedList<T> findPagedList(Query<T> query) {

    int firstRow = query.getFirstRow();
    int maxRows = query.getMaxRows();
    BeanSearchParser<T> parser = find(query);
    try {
      List<T> list = parser.read();
      return new HitsPagedList<T>(firstRow, maxRows, list, parser.getTotal());

    } catch (IOException e) {
      throw new PersistenceIOException(e);
    }
  }

  public <T> List<T> findList(Query<T> query) {

    BeanSearchParser<T> parser = find(query);
    try {
      return parser.read();
    } catch (IOException e) {
      throw new PersistenceIOException(e);
    }
  }

  private <T> BeanSearchParser<T> find(Query<T> query) {

    BeanType<T> desc = server.getBeanType(query.getBeanType());
    BeanDocType beanDocType = desc.docStore();

    SpiQuery<T> spiQuery = (SpiQuery<T>)query;
    JsonReadOptions options = getJsonReadOptions(spiQuery);

    try {
      JsonParser json = postQuery(false, beanDocType, spiQuery);
      JsonBeanReader<T> beanReader = server.json().createBeanReader(query.getBeanType(), json, options);
      return new BeanSearchParser<T>(json, desc, beanReader, spiQuery.getLazyLoadMany());

    } catch (IOException e) {
      throw new PersistenceIOException(e);
    }
  }

  private <T> JsonReadOptions getJsonReadOptions(SpiQuery<T> spiQuery) {

    JsonReadOptions options = new JsonReadOptions();
    if (!spiQuery.isDisableLazyLoading()) {
      options.setEnableLazyLoading(true);
    }
    options.setPersistenceContext(spiQuery.getPersistenceContext());
    return options;
  }


  private JsonParser postQuery(boolean scroll, BeanDocType type, SpiQuery<?> query) throws IOException, DocumentNotFoundException {

    IndexMessageResponse response = messageSender.postQuery(scroll, type.getIndexType(), type.getIndexName(), query.asElasticQuery());
    switch (response.getCode()) {
      case 404:
        throw new DocumentNotFoundException("404 for query?");
      case 200:
        return jsonFactory.createParser(response.getBody());
      default:
        throw new IOException("Unhandled response code " + response.getCode() + " body:" + response.getBody());
    }
  }

  private JsonParser getSource(String indexType, String indexName, Object docId) throws IOException, DocumentNotFoundException {

    IndexMessageResponse response = messageSender.getDocSource(indexType, indexName, docId.toString());
    switch (response.getCode()) {
      case 404:
        throw new DocumentNotFoundException("404 for docId:" + docId);
      case 200:
        return jsonFactory.createParser(response.getBody());
      default:
        throw new IOException("Unhandled response code " + response.getCode() + " body:" + response.getBody());
    }
  }

  private JsonParser getScroll(String scrollId) throws IOException, DocumentNotFoundException {

    IndexMessageResponse response = messageSender.getScroll(scrollId);
    switch (response.getCode()) {
      case 404:
        throw new DocumentNotFoundException("404 for scrollId:" + scrollId);
      case 200:
        return jsonFactory.createParser(response.getBody());
      default:
        throw new IOException("Unhandled response code " + response.getCode() + " body:" + response.getBody());
    }
  }

  public long copyIndexSince(BeanType<?> desc, String newIndex, ElasticBatchUpdate txn, long epochMillis) throws IOException {

    CopyRaw copyRaw = new CopyRaw(txn, desc.docStore().getIndexType(), newIndex);

    Query<?> query = server.createQuery(desc.getBeanType());

    if (epochMillis > 0) {
      Property whenModified = desc.getWhenModifiedProperty();
      if (whenModified != null) {
        query.where().ge(whenModified.getName(), epochMillis);
      }
    }

    long count = findEachRawSource(query, copyRaw);

    logger.info("total [{}] entries copied to index:{}", count, newIndex);
    if (count != 0) {
      txn.flush();
    }

    return count;
  }

  public <T> long findEachRawSource(Query<T> query, QueryEachConsumer<RawSource> consumer) {

    long count = 0;
    Class<T> beanType = query.getBeanType();
    BeanType<T> desc = server.getBeanType(beanType);
    BeanDocType beanDocType = desc.docStore();
    SpiQuery<T> spiQuery = (SpiQuery<T>)query;
    Set<String> scrollIds = new LinkedHashSet<String>();

    try {
      JsonParser initialJson = postQuery(true, beanDocType, spiQuery);

      RawSourceReader reader = new RawSourceReader(initialJson);
      List<RawSource> list = reader.read();

      for (RawSource bean : list) {
        count++;
        consumer.accept(bean);
      }

      String scrollId = reader.getScrollId();
      scrollIds.add(scrollId);

      if (!reader.allHitsRead()) {
        while (true) {
          JsonParser moreJson = getScroll(scrollId);
          RawSourceReader moreReader = new RawSourceReader(moreJson);
          List<RawSource> moreList = moreReader.read();
          for (RawSource bean : moreList) {
            count++;
            consumer.accept(bean);
          }

          scrollId = moreReader.getScrollId();
          scrollIds.add(scrollId);

          if (moreReader.zeroHits()) {
            break;
          }
        }
      }

      return count;

    } catch (IOException e) {
      throw new PersistenceIOException(e);

    } finally {
      clearScrollIds(scrollIds);
    }
  }

  public <T> void findEach(Query<T> query, QueryEachConsumer<T> consumer) {

    Class<T> beanType = query.getBeanType();
    BeanType<T> desc = server.getBeanType(beanType);
    BeanDocType beanDocType = desc.docStore();

    SpiQuery<T> spiQuery = (SpiQuery<T>) query;
    JsonReadOptions options = getJsonReadOptions(spiQuery);

    Set<String> scrollIds = new LinkedHashSet<String>();
    try {
      JsonParser initialJson = postQuery(true, beanDocType, spiQuery);
      JsonBeanReader<T> beanReader = server.json().createBeanReader(query.getBeanType(), initialJson, options);
      BeanSearchParser<T> initialParser = new BeanSearchParser<T>(initialJson, desc, beanReader, spiQuery.getLazyLoadMany());

      List<T> list = initialParser.read();
      for (T bean : list) {
        consumer.accept(bean);
      }

      String scrollId = initialParser.getScrollId();
      scrollIds.add(scrollId);

      if (!initialParser.allHitsRead()) {
        while (true) {
          JsonParser moreJson = getScroll(scrollId);
          beanReader = beanReader.forJson(moreJson);
          BeanSearchParser<T> moreParser = new BeanSearchParser<T>(moreJson, desc, beanReader, spiQuery.getLazyLoadMany());

          List<T> moreList = moreParser.read();
          for (T bean : moreList) {
            consumer.accept(bean);
          }

          scrollId = moreParser.getScrollId();
          scrollIds.add(scrollId);

          if (moreParser.zeroHits()) {
            break;
          }
        }
      }

    } catch (IOException e) {
      throw new PersistenceIOException(e);

    } finally {
      clearScrollIds(scrollIds);
    }

  }

  public <T> T getById(Class<T> beanType, Object id) {
    BeanType<T> desc = server.getBeanType(beanType);
    if (desc == null) {
      throw new IllegalArgumentException("Type [" + beanType + "] does not appear to be an entity bean type?");
    }

    BeanDocType beanDocType = desc.docStore();
    try {
      JsonParser parser = getSource(beanDocType.getIndexType(), beanDocType.getIndexName(), id);
      T bean = desc.jsonRead(parser, null, null);
      desc.setBeanId(bean, id);

      return bean;

    } catch (DocumentNotFoundException e) {
      // this is treated like findUnique() so returning null
      return null;

    } catch (IOException e) {
      throw new PersistenceIOException(e);
    }
  }


  private void clearScrollIds(Set<String> scrollIds) {
    try {
      messageSender.clearScrollIds(scrollIds);
    } catch (IOException e) {
      logger.error("Error trying to clear scrollIds: " + scrollIds, e);
    }
  }

}
