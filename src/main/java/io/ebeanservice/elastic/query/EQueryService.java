package io.ebeanservice.elastic.query;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import io.ebean.PagedList;
import io.ebean.PersistenceIOException;
import io.ebean.Query;
import io.ebean.plugin.BeanDocType;
import io.ebean.plugin.BeanType;
import io.ebean.plugin.Property;
import io.ebean.plugin.SpiServer;
import io.ebean.text.json.JsonBeanReader;
import io.ebean.text.json.JsonContext;
import io.ebean.text.json.JsonReadOptions;
import io.ebeaninternal.api.SpiQuery;
import io.ebeaninternal.api.SpiTransaction;
import io.ebeanservice.docstore.api.DocQueryRequest;
import io.ebeanservice.docstore.api.DocumentNotFoundException;
import io.ebeanservice.docstore.api.RawDoc;
import io.ebeanservice.elastic.bulk.BulkUpdate;
import io.ebeanservice.elastic.querywriter.ElasticDocQueryContext;
import io.ebeanservice.elastic.querywriter.ElasticJsonContext;
import io.ebeanservice.elastic.search.HitsPagedList;
import io.ebeanservice.elastic.search.bean.BeanSearchParser;
import io.ebeanservice.elastic.search.rawsource.RawSourceCopier;
import io.ebeanservice.elastic.search.rawsource.RawSourceEach;
import io.ebeanservice.elastic.support.IndexMessageSender;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Predicate;

/**
 * Internal query service.
 */
public class EQueryService {

  private static final Logger logger = LoggerFactory.getLogger(EQueryService.class);

  private final SpiServer server;

  private final EQuerySend send;

  private final JsonContext jsonContext;

  private final ElasticJsonContext elasticJsonContext;

  public EQueryService(SpiServer server, JsonFactory jsonFactory, IndexMessageSender messageSender) {
    this.server = server;
    this.jsonContext = server.json();
    this.send = new EQuerySend(jsonFactory, messageSender);
    this.elasticJsonContext = new ElasticJsonContext(jsonContext);
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

      return new HitsPagedList<>(firstRow, maxRows, list, parser.getTotal());

    } catch (IOException e) {
      throw new PersistenceIOException(e);
    }
  }

  /**
   * Execute the findList query request.
   */
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

    try {
      JsonParser json = send.findHits(indexNameType(query), asJson(query));
      return createBeanParser(query, json, readOptions);

    } catch (IOException e) {
      throw new PersistenceIOException(e);
    }
  }

  /**
   * Execute the findEachWhile query request.
   */
  public <T> void findEachWhile(DocQueryRequest<T> request, Predicate<T> consumer) {
    processEachWhile(consumer, createQueryEach(request));
  }

  /**
   * Execute the findEach query request.
   */
  public <T> void findEach(DocQueryRequest<T> request, Consumer<T> consumer) {

    EQueryEach<T> each = createQueryEach(request);
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

  private <T> EQueryEach<T> createQueryEach(DocQueryRequest<T> request) {
    SpiQuery<T> query = request.getQuery();
    String nameType = indexNameType(query);
    String jsonQuery = asJson(query);
    return new EQueryEach<>(request, send, jsonContext, nameType, jsonQuery);
  }

  /**
   * Execute the find by id query request.
   */
  public <T> T findById(DocQueryRequest<T> request) {

    SpiQuery<T> query = request.getQuery();

    SpiTransaction transaction = request.getTransaction();
    if (transaction == null) {
      transaction = new EQueryTransaction();
      request.setTransaction(transaction);
      // set tenantId
    }

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
      JsonParser parser = send.findById(indexNameType(beanDocType), id);

      JsonBeanReader<T> reader = new EQuery<>(desc, jsonContext, options).createReader(parser);
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

  /**
   * Execute copyIndexSince which does a raw index to index copy.
   */
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

  /**
   * Execute copyIndexSince which does a raw index to index copy.
   */
  public long copyIndexSince(SpiQuery<?> query, String newIndex, BulkUpdate txn) throws IOException {

    int maxRows = query.getMaxRows();
    if (maxRows == 0) {
      // default to fetch 100 at a time
      query.setMaxRows(100);
    }

    BeanType<?> desc = query.getBeanDescriptor();
    long count = findEachRawSource(query, new RawSourceCopier(txn, desc.docStore().getIndexType(), newIndex));
    logger.debug("total [{}] entries copied to index:{}", count, newIndex);
    return count;
  }

  /**
   * Execute raw find each query.
   */
  public void findEachRaw(String indexNameType, String rawQuery, Consumer<RawDoc> consumer) {
    processEach(consumer, indexNameType, rawQuery);
  }

  /**
   * Execute raw find each query.
   */
  public void findEachWhile(String nameType, String jsonQuery, Predicate<RawDoc> consumer) {
    processEachWhile(consumer, new RawSourceEach(send, nameType, jsonQuery));
  }

  private <T> long findEachRawSource(Query<T> query, Consumer<RawDoc> consumer) {
    SpiQuery<T> spiQuery = (SpiQuery<T>) query;
    return processEach(consumer, indexNameType(spiQuery), asJson(spiQuery));
  }

  private long processEach(Consumer<RawDoc> consumer, String nameType, String jsonQuery) {
    RawSourceEach each = new RawSourceEach(send, nameType, jsonQuery);
    try {
      if (each.consumeInitial(consumer)) {
        while (each.consumeNext(consumer)) {
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

  private <T> void processEachWhile(Predicate<T> consumer, EConsumeWhile<T> each) {
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
   * Return the bean type specific parser used to read the search results.
   */
  private <T> BeanSearchParser<T> createBeanParser(SpiQuery<T> query, JsonParser json, JsonReadOptions options) {
    return new EQuery<>(query, jsonContext, options).createParser(json);
  }

  /**
   * Return the query as ElasticSearch JSON format.
   */
  private String asJson(SpiQuery<?> query) {
    return ElasticDocQueryContext.asJson(elasticJsonContext, query);
  }

  private String indexNameType(BeanDocType type) {
    return type.getIndexName() + "/" + type.getIndexType();
  }

  private String indexNameType(SpiQuery<?> query) {
    String docIndexName = query.getDocIndexName();
    if (docIndexName != null) {
      return docIndexName;
    } else {
      return indexNameType(query.getBeanDescriptor().docStore());
    }
  }
}
