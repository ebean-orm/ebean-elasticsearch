package com.avaje.ebeanservice.elastic;

import com.avaje.ebean.DocStoreQueueEntry;
import com.avaje.ebean.DocumentStore;
import com.avaje.ebean.PersistenceIOException;
import com.avaje.ebean.Query;
import com.avaje.ebean.QueryEachConsumer;
import com.avaje.ebean.plugin.SpiBeanType;
import com.avaje.ebean.plugin.SpiServer;
import com.avaje.ebeaninternal.api.SpiQuery;
import com.avaje.ebeanservice.docstore.api.DocStoreQueryUpdate;
import com.avaje.ebeanservice.elastic.index.IndexService;
import com.avaje.ebeanservice.elastic.support.ElasticBatchUpdate;
import com.avaje.ebeanservice.elastic.support.IndexMessageSender;
import com.avaje.ebeanservice.elastic.updategroup.ConvertToGroups;
import com.avaje.ebeanservice.elastic.updategroup.ProcessGroup;
import com.avaje.ebeanservice.elastic.updategroup.UpdateGroup;
import com.fasterxml.jackson.core.JsonFactory;

import java.io.IOException;
import java.util.Collection;
import java.util.List;

/**
 * ElasticSearch based document store.
 */
public class ElasticDocumentStore implements DocumentStore {


  private final SpiServer server;

  private final ElasticUpdateProcessor updateProcessor;

  private final ElasticQueryService queryService;

  private final IndexService indexService;

  public ElasticDocumentStore(SpiServer server, ElasticUpdateProcessor updateProcessor, IndexMessageSender messageSender, JsonFactory jsonFactory) {
    this.server = server;
    this.updateProcessor = updateProcessor;
    this.queryService = new ElasticQueryService(server, jsonFactory, messageSender);
    this.indexService = new IndexService(server, jsonFactory);
  }

  @Override
  public void process(List<DocStoreQueueEntry> entries) throws IOException {

    Collection<UpdateGroup> groups = ConvertToGroups.groupByQueueId(entries);
    ElasticBatchUpdate txn = updateProcessor.createBatchUpdate(0);

    try {
      for (UpdateGroup group : groups) {
        SpiBeanType<?> desc = server.getBeanTypeForQueueId(group.getQueueId());
        ProcessGroup.process(server, desc, group, txn);
      }

      txn.flush();

    } catch (IOException e) {
      throw new PersistenceIOException(e);
    }
  }


  @Override
  public <T> void indexByQuery(Query<T> query) {
    indexByQuery(query, 0);
  }

  @Override
  public <T> void indexByQuery(Query<T> query, int bulkBatchSize) {

    SpiQuery<T> spiQuery = (SpiQuery<T>) query;
    Class<T> beanType = spiQuery.getBeanType();

    SpiBeanType<T> beanDescriptor = server.getBeanType(beanType);
    if (beanDescriptor == null) {
      throw new IllegalArgumentException("Type [" + beanType + "] does not appear to be an entity bean type?");
    }

    try {
      DocStoreQueryUpdate<T> update = updateProcessor.createQueryUpdate(beanDescriptor, bulkBatchSize);
      indexByQuery(beanDescriptor, query, update);
      update.flush();

    } catch (IOException e) {
      throw new PersistenceIOException(e);
    }
  }


  private <T> void indexByQuery(final SpiBeanType<T> desc, Query<T> query, final DocStoreQueryUpdate<T> queryUpdate) throws IOException {

    desc.docStoreApplyPath(query);
    query.setLazyLoadBatchSize(100);
    query.findEach(new QueryEachConsumer<T>() {
      @Override
      public void accept(T bean) {
        Object idValue = desc.getBeanId(bean);
        try {
          queryUpdate.store(idValue, bean);
        } catch (Exception e) {
          throw new PersistenceIOException("Error performing query update to doc store", e);
        }
      }
    });
  }

  @Override
  public <T> void findEach(Query<T> query, QueryEachConsumer<T> consumer) {
    queryService.findEach(query, consumer);
  }


  @Override
  public <T> List<T> findList(Query<T> query) {
    return queryService.findList(query);
  }

  @Override
  public <T> T getById(Class<T> beanType, Object id) {
    return queryService.getById(beanType, id);
  }

  public void onStartup() {


    indexService.createIndexes();
  }
}
