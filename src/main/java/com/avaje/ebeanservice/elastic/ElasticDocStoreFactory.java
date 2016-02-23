package com.avaje.ebeanservice.elastic;

import com.avaje.ebean.DocumentStore;
import com.avaje.ebean.config.DocStoreConfig;
import com.avaje.ebean.config.ServerConfig;
import com.avaje.ebean.plugin.SpiServer;
import com.avaje.ebean.plugin.SpiServerPlugin;
import com.avaje.ebeaninternal.server.deploy.BeanDescriptor;
import com.avaje.ebeaninternal.server.deploy.meta.DeployBeanDescriptor;
import com.avaje.ebeanservice.docstore.api.DocStoreBeanAdapter;
import com.avaje.ebeanservice.docstore.api.DocStoreFactory;
import com.avaje.ebeanservice.docstore.api.DocStoreIntegration;
import com.avaje.ebeanservice.docstore.api.DocStoreUpdateProcessor;
import com.avaje.ebeanservice.elastic.support.BaseHttpMessageSender;
import com.avaje.ebeanservice.elastic.support.BaseIndexQueueWriter;
import com.avaje.ebeanservice.elastic.support.IndexMessageSender;
import com.avaje.ebeanservice.elastic.support.IndexQueueWriter;
import com.fasterxml.jackson.core.JsonFactory;

/**
 * Factory that creates the document store integration components.
 */
public class ElasticDocStoreFactory implements DocStoreFactory {

  @Override
  public <T> DocStoreBeanAdapter<T> createAdapter(BeanDescriptor<T> desc, DeployBeanDescriptor<T> deploy) {
    return new ElasticDocStoreBeanAdapter<T>(desc, deploy);
  }

  @Override
  public DocStoreIntegration create(SpiServer server) {

    ServerConfig serverConfig = server.getServerConfig();

    Object objectMapper = serverConfig.getObjectMapper();

    DocStoreConfig docStoreConfig = serverConfig.getDocStoreConfig();

    JsonFactory jsonFactory = new JsonFactory();
    IndexQueueWriter indexQueueWriter = new BaseIndexQueueWriter(server, "eb_elastic_queue");
    IndexMessageSender messageSender = new BaseHttpMessageSender(docStoreConfig.getUrl());

    ElasticUpdateProcessor updateProcessor = new ElasticUpdateProcessor(indexQueueWriter, jsonFactory, objectMapper, messageSender, docStoreConfig.getBulkBatchSize());

    ElasticDocumentStore docStore = new ElasticDocumentStore(server, updateProcessor, messageSender, jsonFactory);

    return new Components(updateProcessor, docStore);
  }


  static class Components implements DocStoreIntegration, SpiServerPlugin {

    final ElasticUpdateProcessor updateProcessor;
    final ElasticDocumentStore documentStore;

    Components(ElasticUpdateProcessor updateProcessor, ElasticDocumentStore documentStore) {
      this.updateProcessor = updateProcessor;
      this.documentStore = documentStore;
    }

    @Override
    public DocStoreUpdateProcessor updateProcessor() {
      return updateProcessor;
    }

    @Override
    public DocumentStore documentStore() {
      return documentStore;
    }

    @Override
    public void configure(SpiServer server) {

    }

    @Override
    public void online(boolean online) {

      if (online) {
        documentStore.onStartup();
      }
    }

    @Override
    public void shutdown() {

    }
  }
}
