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

    SpiQuery<T> spiQuery = (SpiQuery<T>) query;
    BeanType<T> desc = spiQuery.getBeanDescriptor();
    try {
      JsonParser json = send.findHits(desc.docStore(), spiQuery);
      return createBeanParser(spiQuery, json);

    } catch (IOException e) {
      throw new PersistenceIOException(e);
    }
  }

  public <T> void findEachWhile(Query<T> query, QueryEachWhileConsumer<T> consumer) {

    EQueryEach<T> each = new EQueryEach<T>((SpiQuery<T>) query, send, jsonContext);
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

  /**
   * Execute a scroll query iterating through the results.
   */
  public <T> void findEach(Query<T> query, QueryEachConsumer<T> consumer) {

    EQueryEach<T> each = new EQueryEach<T>((SpiQuery<T>) query, send, jsonContext);
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

      JsonReadOptions options = new JsonReadOptions().setEnableLazyLoading(true);

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


    SpiQuery<?> query = (SpiQuery<?>)server.createQuery(desc.getBeanType());
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

      if (each.consumeInitial(consumer, beanDocType, query)) {
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
  private <T> BeanSearchParser<T> createBeanParser(SpiQuery<T> query, JsonParser json) {
    return new EQuery<T>(query, jsonContext).createParser(json);
  }

}
