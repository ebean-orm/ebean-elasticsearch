package com.avaje.ebeanservice.elastic.query;

import com.avaje.ebean.PagedList;
import com.avaje.ebean.PersistenceIOException;
import com.avaje.ebean.Query;
import com.avaje.ebean.QueryEachConsumer;
import com.avaje.ebean.QueryEachWhileConsumer;
import com.avaje.ebean.plugin.BeanDocType;
import com.avaje.ebean.plugin.BeanType;
import com.avaje.ebean.plugin.Property;
import com.avaje.ebean.plugin.SpiServer;
import com.avaje.ebean.text.json.JsonBeanReader;
import com.avaje.ebean.text.json.JsonContext;
import com.avaje.ebean.text.json.JsonReadOptions;
import com.avaje.ebeaninternal.api.SpiQuery;
import com.avaje.ebeanservice.docstore.api.DocQueryRequest;
import com.avaje.ebeanservice.docstore.api.DocumentNotFoundException;
import com.avaje.ebeanservice.elastic.bulk.BulkUpdate;
import com.avaje.ebeanservice.elastic.search.HitsPagedList;
import com.avaje.ebeanservice.elastic.search.bean.BeanSearchParser;
import com.avaje.ebeanservice.elastic.search.rawsource.RawSource;
import com.avaje.ebeanservice.elastic.search.rawsource.RawSourceCopier;
import com.avaje.ebeanservice.elastic.search.rawsource.RawSourceEach;
import com.avaje.ebeanservice.elastic.support.IndexMessageSender;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

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
    this.send = new EQuerySend(jsonContext, jsonFactory, messageSender);
  }

  /**
   * Execute the query returning a PagedList of hits.
   */
  public <T> PagedList<T> findPagedList(DocQueryRequest<T> request) {

    SpiQuery<T> query = request.getQuery();
    int firstRow = query.getFirstRow();
    int maxRows = query.getMaxRows();

    BeanSearchParser<T> parser = findHits(query, request.createJsonReadOptions());
    try {
      List<T> list = parser.read();
      request.executeSecondaryQueries(false);

      return new HitsPagedList<T>(firstRow, maxRows, list, parser.getTotal());

    } catch (IOException e) {
      throw new PersistenceIOException(e);
    }
  }

  public <T> List<T> findList(DocQueryRequest<T> request) {

    BeanSearchParser<T> parser = findHits(request.getQuery(), request.createJsonReadOptions());
    try {
      List<T> list = parser.read();
      request.executeSecondaryQueries(false);
      return list;

    } catch (IOException e) {
      throw new PersistenceIOException(e);
    }
  }

  private <T> BeanSearchParser<T> findHits(SpiQuery<T> query, JsonReadOptions readOptions) {

    BeanType<T> desc = query.getBeanDescriptor();
    try {
      JsonParser json = send.findHits(desc.docStore(), query);
      return createBeanParser(query, json, readOptions);

    } catch (IOException e) {
      throw new PersistenceIOException(e);
    }
  }

  public <T> void findEachWhile(DocQueryRequest<T> request, QueryEachWhileConsumer<T> consumer) {

    EQueryEach<T> each = new EQueryEach<T>(request, send, jsonContext);
    try {
      if (!each.consumeInitialWhile(consumer)) {
        return;
      }
      while (true) {
        if (!each.consumeMoreWhile(consumer)) {
          return;
        }
      }
    } catch (IOException e) {
      throw new PersistenceIOException(e);

    } finally {
      each.clearScrollIds();
    }
  }

  public <T> void findEach(DocQueryRequest<T> request, QueryEachConsumer<T> consumer) {

    EQueryEach<T> each = new EQueryEach<T>(request, send, jsonContext);
    try {
      if (each.consumeInitial(consumer)) {
        while (true) {
          if (!each.consumeMore(consumer)) {
            break;
          }
        }
      }
    } catch (IOException e) {
      throw new PersistenceIOException(e);

    } finally {
      each.clearScrollIds();
    }
  }

  public <T> T findById(DocQueryRequest<T> request) {

    SpiQuery<T> query = request.getQuery();

    T bean = findById(query.getBeanDescriptor(), query.getId(), request.createJsonReadOptions());
    request.executeSecondaryQueries(false);
    return bean;
  }

  /**
   * Execute find by id.
   */
  private <T> T findById(BeanType<T> desc, Object id, JsonReadOptions options) {

    BeanDocType beanDocType = desc.docStore();
    try {
      JsonParser parser = send.findById(beanDocType.getIndexType(), beanDocType.getIndexName(), id);

      JsonBeanReader<T> reader = new EQuery<T>(desc, jsonContext, options).createReader(parser);
      T bean = reader.read();
      desc.setBeanId(bean, id);
      // register with persistence context and load context
      reader.persistenceContextPut(desc.getBeanId(bean), bean);
      return bean;

    } catch (DocumentNotFoundException e) {
      // this is treated like findUnique() so returning null
      return null;

    } catch (IOException e) {
      throw new PersistenceIOException(e);
    }
  }

  public long copyIndexSince(BeanType<?> desc, String newIndex, BulkUpdate txn, long epochMillis) throws IOException {

    SpiQuery<?> query = (SpiQuery<?>) server.createQuery(desc.getBeanType());
    if (epochMillis > 0) {
      Property whenModified = desc.getWhenModifiedProperty();
      if (whenModified != null) {
        query.where().ge(whenModified.getName(), epochMillis);
      }
    }

    return copyIndexSince(query, newIndex, txn);
  }

  public long copyIndexSince(SpiQuery<?> query, String newIndex, BulkUpdate txn) throws IOException {

    BeanType<?> desc = query.getBeanDescriptor();
    long count = findEachRawSource(query, new RawSourceCopier(txn, desc.docStore().getIndexType(), newIndex));
    logger.debug("total [{}] entries copied to index:{}", count, newIndex);

    return count;
  }

  /**
   * Execute a scroll query using RawSource.
   */
  public <T> long findEachRawSource(Query<T> query, QueryEachConsumer<RawSource> consumer) {

    SpiQuery<T> spiQuery = (SpiQuery<T>) query;
    BeanType<T> desc = spiQuery.getBeanDescriptor();
    BeanDocType beanDocType = desc.docStore();

    RawSourceEach each = new RawSourceEach(send);
    try {

      if (each.consumeInitial(consumer, beanDocType, spiQuery)) {
        while (!each.consumeNext(consumer)) {
          // continue
        }
      }
      return each.getTotalCount();
    } catch (IOException e) {
      throw new PersistenceIOException(e);

    } finally {
      each.clearScrollIds();
    }
  }

  /**
   * Return the bean type specific parser used to read the search results.
   */
  private <T> BeanSearchParser<T> createBeanParser(SpiQuery<T> query, JsonParser json, JsonReadOptions options) {
    return new EQuery<T>(query, jsonContext, options).createParser(json);
  }

}
