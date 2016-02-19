package com.avaje.ebeanservice.elastic;

import com.avaje.ebean.DocStoreQueueEntry;
import com.avaje.ebean.DocumentStore;
import com.avaje.ebean.PersistenceIOException;
import com.avaje.ebean.Query;
import com.avaje.ebean.QueryEachConsumer;
import com.avaje.ebean.plugin.SpiBeanType;
import com.avaje.ebean.plugin.SpiServer;
import com.avaje.ebean.text.json.JsonContext;
import com.avaje.ebeaninternal.api.SpiQuery;
import com.avaje.ebeanservice.docstore.api.DocStoreQueryUpdate;
import com.avaje.ebeanservice.docstore.api.DocumentNotFoundException;
import com.avaje.ebeanservice.elastic.search.BasicFieldsListener;
import com.avaje.ebeanservice.elastic.search.BeanSourceListener;
import com.avaje.ebeanservice.elastic.search.SearchResultParser;
import com.avaje.ebeanservice.elastic.search.SearchRow;
import com.avaje.ebeanservice.elastic.support.ElasticBatchUpdate;
import com.avaje.ebeanservice.elastic.support.IndexMessageResponse;
import com.avaje.ebeanservice.elastic.support.IndexMessageSender;
import com.avaje.ebeanservice.elastic.updategroup.ConvertToGroups;
import com.avaje.ebeanservice.elastic.updategroup.ProcessGroup;
import com.avaje.ebeanservice.elastic.updategroup.UpdateGroup;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;

import java.io.IOException;
import java.util.Collection;
import java.util.List;

/**
 * ElasticSearch based document store.
 */
public class ElasticDocumentStore implements DocumentStore {

  private final SpiServer server;

  private final ElasticUpdateProcessor updateProcessor;

  private final IndexMessageSender messageSender;

  private final JsonFactory jsonFactory;

  public ElasticDocumentStore(SpiServer server, ElasticUpdateProcessor updateProcessor, IndexMessageSender messageSender, JsonFactory jsonFactory) {
    this.server = server;
    this.updateProcessor = updateProcessor;
    this.messageSender = messageSender;
    this.jsonFactory = jsonFactory;
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
  public <T> List<T> findList(Query<T> query) {

    Class<T> beanType = query.getBeanType();
    SpiBeanType<T> desc = server.getBeanType(beanType);

    try {
      JsonParser jp = postQuery(desc.getDocStoreIndexType(), desc.getDocStoreIndexName(), query.asElasticQuery());


      BasicFieldsListener basicFieldsListener = new BasicFieldsListener();
      BeanSourceListener<T> sourceListener = new BeanSourceListener<T>(desc);
      SearchResultParser resultParser = new SearchResultParser(jp, sourceListener);

      resultParser.read();

      List<SearchRow> rows = basicFieldsListener.getRows();
      List<T> list = sourceListener.getList();

      return list;

    } catch (IOException e) {
      throw new PersistenceIOException(e);
    }
  }

  @Override
  public <T> T getById(Class<T> beanType, Object id) {

    SpiBeanType<T> desc = server.getBeanType(beanType);
    if (desc == null) {
      throw new IllegalArgumentException("Type [" + beanType + "] does not appear to be an entity bean type?");
    }

    try {
      JsonParser parser = getSource(desc.getDocStoreIndexType(), desc.getDocStoreIndexName(), id);
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

  private JsonParser postQuery(String indexType, String indexName, String jsonQuery) throws IOException, DocumentNotFoundException {


    IndexMessageResponse response = messageSender.postQuery(indexType, indexName, jsonQuery);
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

}
