package io.ebeanservice.elastic;

import io.ebean.DocumentStore;
import io.ebean.config.DocStoreConfig;
import io.ebean.config.ServerConfig;
import io.ebean.plugin.SpiServer;
import io.ebean.plugin.Plugin;
import io.ebeaninternal.server.deploy.BeanDescriptor;
import io.ebeaninternal.server.deploy.meta.DeployBeanDescriptor;
import io.ebeanservice.docstore.api.DocStoreBeanAdapter;
import io.ebeanservice.docstore.api.DocStoreFactory;
import io.ebeanservice.docstore.api.DocStoreIntegration;
import io.ebeanservice.docstore.api.DocStoreUpdateProcessor;
import io.ebeanservice.elastic.support.BaseHttpMessageSender;
import io.ebeanservice.elastic.support.BaseIndexQueueWriter;
import io.ebeanservice.elastic.support.IndexMessageSender;
import io.ebeanservice.elastic.support.IndexQueueWriter;
import com.fasterxml.jackson.core.JsonFactory;

/**
 * Factory that creates the document store integration components.
 */
public class ElasticDocStoreFactory implements DocStoreFactory {

  @Override
  public <T> DocStoreBeanAdapter<T> createAdapter(BeanDescriptor<T> desc, DeployBeanDescriptor<T> deploy) {
    return new ElasticDocStoreBeanAdapter<>(desc, deploy);
  }

  @Override
  public DocStoreIntegration create(SpiServer server) {

    ServerConfig serverConfig = server.getServerConfig();

    Object objectMapper = serverConfig.getObjectMapper();

    DocStoreConfig docStoreConfig = serverConfig.getDocStoreConfig();

    JsonFactory jsonFactory = new JsonFactory();
    IndexQueueWriter indexQueueWriter = new BaseIndexQueueWriter(server, "eb_elastic_queue");
    IndexMessageSender messageSender = new BaseHttpMessageSender(docStoreConfig);

    ElasticUpdateProcessor updateProcessor = new ElasticUpdateProcessor(server, indexQueueWriter, jsonFactory, objectMapper, messageSender, docStoreConfig.getBulkBatchSize());

    ElasticDocumentStore docStore = new ElasticDocumentStore(server, updateProcessor, messageSender, jsonFactory);

    return new Components(updateProcessor, docStore);
  }


  static class Components implements DocStoreIntegration, Plugin {

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
        updateProcessor.onStartup();
        documentStore.onStartup();
      }
    }

    @Override
    public void shutdown() {

    }
  }
}
