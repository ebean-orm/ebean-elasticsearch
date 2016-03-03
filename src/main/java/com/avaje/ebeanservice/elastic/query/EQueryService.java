package com.avaje.ebeanservice.elastic.query;

import com.avaje.ebean.PagedList;
import com.avaje.ebean.PersistenceIOException;
import com.avaje.ebean.Query;
import com.avaje.ebean.QueryEachConsumer;
import com.avaje.ebean.plugin.BeanDocType;
import com.avaje.ebean.plugin.BeanType;
import com.avaje.ebean.plugin.Property;
import com.avaje.ebean.plugin.SpiServer;
import com.avaje.ebean.text.json.JsonBeanReader;
import com.avaje.ebean.text.json.JsonContext;
import com.avaje.ebean.text.json.JsonReadOptions;
import com.avaje.ebeaninternal.api.SpiQuery;
import com.avaje.ebeanservice.docstore.api.DocumentNotFoundException;
import com.avaje.ebeanservice.elastic.bulk.BulkUpdate;
import com.avaje.ebeanservice.elastic.search.bean.BeanSearchParser;
import com.avaje.ebeanservice.elastic.search.HitsPagedList;
import com.avaje.ebeanservice.elastic.search.rawsource.RawSourceCopier;
import com.avaje.ebeanservice.elastic.search.rawsource.RawSource;
import com.avaje.ebeanservice.elastic.search.rawsource.RawSourceReader;
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
 * Internal query service.
 */
public class EQueryService {

  private static final Logger logger = LoggerFactory.getLogger(EQueryService.class);

  private final SpiServer server;

  private final EQuerySend send;

  private final JsonContext jsonContext;

  public EQueryService(SpiServer server, JsonFactory jsonFactory, IndexMessageSender messageSender) {
    this.server = server;
    this.jsonContext = server.json();
    this.send = new EQuerySend(jsonFactory, messageSender);
  }

  /**
   * Execute the query returning a PagedList of hits.
   */
  public <T> PagedList<T> findPagedList(Query<T> query) {

    int firstRow = query.getFirstRow();
    int maxRows = query.getMaxRows();
    BeanSearchParser<T> parser = findHits(query);
    try {
      List<T> list = parser.read();
      return new HitsPagedList<T>(firstRow, maxRows, list, parser.getTotal());

    } catch (IOException e) {
      throw new PersistenceIOException(e);
    }
  }

  /**
   * Execute the query returning the list of beans.
   */
  public <T> List<T> findList(Query<T> query) {

    BeanSearchParser<T> parser = findHits(query);
    try {
      return parser.read();
    } catch (IOException e) {
      throw new PersistenceIOException(e);
    }
  }

  private <T> BeanSearchParser<T> findHits(Query<T> query) {

    SpiQuery<T> spiQuery = (SpiQuery<T>)query;
    BeanType<T> desc = spiQuery.getBeanDescriptor();

    try {
      JsonParser json = send.findHits(desc.docStore(), spiQuery);
      return createBeanParser(spiQuery, desc, json);

    } catch (IOException e) {
      throw new PersistenceIOException(e);
    }
  }

  /**
   * Execute a scroll query iterating through the results.
   */
  public <T> void findEach(Query<T> query, QueryEachConsumer<T> consumer) {

    SpiQuery<T> spiQuery = (SpiQuery<T>) query;
    BeanType<T> desc = spiQuery.getBeanDescriptor();

    Set<String> scrollIds = new LinkedHashSet<String>();
    try {
      JsonParser initialJson = send.findScroll(desc.docStore(), spiQuery);

      JsonReadOptions options = getJsonReadOptions(spiQuery);
      JsonBeanReader<T> beanReader = jsonContext.createBeanReader(query.getBeanType(), initialJson, options);
      BeanSearchParser<T> initialParser = new BeanSearchParser<T>(initialJson, desc, beanReader, spiQuery.getLazyLoadMany());

      List<T> list = initialParser.read();
      for (T bean : list) {
        consumer.accept(bean);
      }

      String scrollId = initialParser.getScrollId();
      scrollIds.add(scrollId);

      if (!initialParser.allHitsRead()) {
        while (true) {
          JsonParser moreJson = send.findNextScroll(scrollId);
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
      send.clearScrollIds(scrollIds);
    }
  }

  /**
   * Execute find by id.
   */
  public <T> T findById(Class<T> beanType, Object id) {
    BeanType<T> desc = server.getBeanType(beanType);
    if (desc == null) {
      throw new IllegalArgumentException("Type [" + beanType + "] does not appear to be an entity bean type?");
    }

    BeanDocType beanDocType = desc.docStore();
    try {
      JsonParser parser = send.findById(beanDocType.getIndexType(), beanDocType.getIndexName(), id);
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


  public long copyIndexSince(BeanType<?> desc, String newIndex, BulkUpdate txn, long epochMillis) throws IOException {

    RawSourceCopier copyRaw = new RawSourceCopier(txn, desc.docStore().getIndexType(), newIndex);

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

    SpiQuery<T> spiQuery = (SpiQuery<T>)query;
    BeanType<T> desc = spiQuery.getBeanDescriptor();
    BeanDocType beanDocType = desc.docStore();
    Set<String> scrollIds = new LinkedHashSet<String>();

    try {
      JsonParser initialJson = send.findScroll(beanDocType, spiQuery);

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
          JsonParser moreJson = send.findNextScroll(scrollId);
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
      send.clearScrollIds(scrollIds);
    }
  }

  /**
   * Return the bean type specific parser used to read the search results.
   */
  private <T> BeanSearchParser<T> createBeanParser(SpiQuery<T> query, BeanType<T> desc, JsonParser json) {

    JsonReadOptions options = getJsonReadOptions(query);
    JsonBeanReader<T> beanReader = jsonContext.createBeanReader(query.getBeanType(), json, options);
    return new BeanSearchParser<T>(json, desc, beanReader, query.getLazyLoadMany());
  }

  /**
   * Return the JsonReadOptions taking into account lazy loading and persistence context.
   */
  private JsonReadOptions getJsonReadOptions(SpiQuery<?> query) {

    JsonReadOptions options = new JsonReadOptions();
    if (!query.isDisableLazyLoading()) {
      options.setEnableLazyLoading(true);
    }
    options.setPersistenceContext(query.getPersistenceContext());
    return options;
  }
}
