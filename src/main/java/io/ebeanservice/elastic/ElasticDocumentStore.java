package io.ebeanservice.elastic;

import com.fasterxml.jackson.core.JsonFactory;
import io.avaje.applog.AppLog;
import io.ebean.*;
import io.ebean.config.DocStoreConfig;
import io.ebean.docstore.DocQueryContext;
import io.ebean.docstore.RawDoc;
import io.ebean.plugin.BeanType;
import io.ebean.plugin.SpiServer;
import io.ebeaninternal.api.SpiQuery;
import io.ebeanservice.docstore.api.DocStoreQueryUpdate;
import io.ebeanservice.elastic.bulk.BulkUpdate;
import io.ebeanservice.elastic.index.EIndexService;
import io.ebeanservice.elastic.query.EQueryService;
import io.ebeanservice.elastic.support.IndexMessageSender;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Predicate;

/**
 * ElasticSearch based document store.
 */
public class ElasticDocumentStore implements DocumentStore {
  /**
   * Logger that can be used to log Bulk API messages.
   */
  public static final System.Logger BULK = AppLog.getLogger("io.ebean.BULK");

  private final SpiServer server;

  private final ElasticUpdateProcessor updateProcessor;

  private final EQueryService queryService;

  private final EIndexService indexService;

  ElasticDocumentStore(SpiServer server, ElasticUpdateProcessor updateProcessor, IndexMessageSender sender, JsonFactory jsonFactory) {
    this.server = server;
    this.updateProcessor = updateProcessor;
    this.queryService = new EQueryService(server, jsonFactory, sender);
    this.indexService = new EIndexService(server, jsonFactory, sender);
  }

  @Override
  public long process(List<DocStoreQueueEntry> entries) throws IOException {

    BulkUpdate bulk = updateProcessor.createBulkUpdate(0);
    try {
      long count = updateProcessor.processQueue(bulk, entries);
      bulk.flush();
      return count;

    } catch (IOException e) {
      throw new PersistenceIOException(e);
    }
  }

  @Override
  public void indexSettings(String indexName, Map<String, Object> settings) {
    try {
      indexService.indexSettings(indexName, settings);
    } catch (IOException e) {
      throw new PersistenceIOException(e);
    }
  }

  @Override
  public void dropIndex(String newIndex) {
    try {
      indexService.dropIndex(newIndex);
    } catch (IOException e) {
      throw new PersistenceIOException(e);
    }
  }

  @Override
  public void createIndex(String indexName, String alias) {
    try {
      indexService.createIndex(indexName, alias);
    } catch (IOException e) {
      throw new PersistenceIOException(e);
    }
  }

  @Override
  public long copyIndex(Query<?> query, String newIndex, int bulkBatchSize) {
    try {
      BulkUpdate txn = updateProcessor.createBulkUpdate(bulkBatchSize);
      long count = queryService.copyIndexSince((SpiQuery<?>)query, newIndex, txn);
      txn.flush();

      return count;

    } catch (IOException e) {
      throw new PersistenceIOException(e);
    }
  }

  @Override
  public long copyIndex(Class<?> beanType, String newIndex, long epochMillis) {
    BeanType<?> type = checkMapped(server.beanType(beanType));
    try {
      BulkUpdate txn = updateProcessor.createBulkUpdate(0);
      long count = queryService.copyIndexSince(type, newIndex, txn, epochMillis);
      txn.flush();

      return count;

    } catch (IOException e) {
      throw new PersistenceIOException(e);
    }
  }

  @Override
  public long copyIndex(Class<?> beanType, String newIndex) {
    return copyIndex(beanType, newIndex, 0);
  }

  @Override
  public void indexAll(Class<?> beanType) {
    indexByQuery(server.createQuery(beanType));
  }

  @Override
  public <T> void indexByQuery(Query<T> query) {
    indexByQuery(query, 0);
  }

  @Override
  public <T> void indexByQuery(Query<T> query, int bulkBatchSize) {
    SpiQuery<T> spiQuery = (SpiQuery<T>) query;
    BeanType<T> desc = checkMapped(spiQuery.getBeanDescriptor());

    try {
      DocStoreQueryUpdate<T> update = updateProcessor.createQueryUpdate(desc, bulkBatchSize);
      indexByQuery(desc, query, update);
      update.flush();
    } catch (IOException e) {
      throw new PersistenceIOException(e);
    }
  }


  private <T> void indexByQuery(final BeanType<T> desc, Query<T> query, final DocStoreQueryUpdate<T> queryUpdate) throws IOException {
    desc.docStore().applyPath(query);
    query.setLazyLoadBatchSize(100);
    query.findEach(bean -> {
      Object idValue = desc.id(bean);
      try {
        queryUpdate.store(idValue, bean);
      } catch (Exception e) {
        throw new PersistenceIOException("Error performing query update to doc store", e);
      }
    });
  }

  @Override
  public <T> void findEach(DocQueryContext<T> request, Consumer<T> consumer) {
    queryService.findEach(request, consumer);
  }

  @Override
  public <T> void findEachWhile(DocQueryContext<T> request, Predicate<T> consumer) {
    queryService.findEachWhile(request, consumer);
  }

  @Override
  public void findEach(String indexName, String rawQuery, Consumer<RawDoc> consumer) {
    queryService.findEachRaw(indexName, rawQuery, consumer);
  }

  @Override
  public void findEachWhile(String indexName, String rawQuery, Predicate<RawDoc> consumer) {
    queryService.findEachWhile(indexName, rawQuery, consumer);
  }

  @Override
  public <T> List<T> findList(DocQueryContext<T> request) {
    return queryService.findList(request);
  }

  @Override
  public <T> PagedList<T> findPagedList(DocQueryContext<T> request) {
    return queryService.findPagedList(request);
  }

  @Override
  public <T> T find(DocQueryContext<T> request) {
    return queryService.findById(request);
  }

  void onStartup() {

    try {
      DocStoreConfig docStoreConfig = server.config().getDocStoreConfig();
      if (docStoreConfig.isActive()) {
        indexService.createIndexesOnStartup();
      }

    } catch (IOException e) {
      throw new PersistenceIOException(e);
    }
  }

  private <T> BeanType<T> checkMapped(BeanType<T> type) {
    if (type == null) {
      throw new IllegalStateException("No bean type mapping found?");
    }
    if (!type.isDocStoreMapped()) {
      throw new IllegalStateException("No doc store mapping for bean type "+type.fullName());
    }
    return type;
  }
}
