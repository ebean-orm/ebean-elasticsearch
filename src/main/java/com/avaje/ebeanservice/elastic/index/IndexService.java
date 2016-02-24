package com.avaje.ebeanservice.elastic.index;

import com.avaje.ebean.PersistenceIOException;
import com.avaje.ebean.plugin.SpiBeanType;
import com.avaje.ebean.plugin.SpiServer;
import com.avaje.ebeanservice.elastic.support.IndexMessageSender;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.LineNumberReader;
import java.io.StringWriter;
import java.util.List;

/**
 *
 */
public class IndexService {

  final SpiServer server;

  final JsonFactory jsonFactory;

  final IndexMappingsBuilder mappingsBuilder;

  final IndexMessageSender sender;

  public IndexService(SpiServer server, JsonFactory jsonFactory, IndexMessageSender sender) {
    this.server = server;
    this.jsonFactory = jsonFactory;
    this.sender = sender;
    this.mappingsBuilder = new IndexMappingsBuilder(jsonFactory);
  }

  public boolean indexExists(String indexName) throws IOException {
    return sender.indexExists(indexName);
  }

  public void dropIndex(String indexName) throws IOException {
    sender.indexDelete(indexName);
  }

  public void createIndex(String indexName, String alias, String mappingResource) throws IOException {

    String resourcePath = "/index-mapping/" + mappingResource +".mapping.json";

    String rawJsonMapping = readResource(resourcePath);
    if (rawJsonMapping == null) {
      throw new IllegalArgumentException("No resource "+resourcePath+" found in classPath");
    }

    createIndexWithMapping(indexName, alias, rawJsonMapping);
  }

  private String readResource(String mappingResource) {

    InputStream is = this.getClass().getResourceAsStream(mappingResource);
    if (is == null) {
      return null;
    }

    try {
      InputStreamReader reader = new InputStreamReader(is);
      LineNumberReader lineReader = new LineNumberReader(reader);

      StringBuilder buffer = new StringBuilder(300);
      String line = null;
      while ((line = lineReader.readLine()) != null) {
        buffer.append(line);
      }

      return buffer.toString();

    } catch (IOException e) {
      throw new PersistenceIOException(e);
    }
  }

  public void createIndexWithMapping(String indexName, String alias, String jsonMapping) throws IOException {

    if (indexExists(indexName)) {
      dropIndex(indexName);
    }
    if (alias != null) {
      if (indexExists(alias)) {
        dropIndex(alias);
      }
    }
    sender.indexCreate(indexName, jsonMapping);

    if (alias != null) {
      String aliasJson = asJson(new AliasChanges().add(indexName, alias));
      sender.indexAlias(aliasJson);
    }
  }

  private String asJson(AliasChanges aliasChanges) throws IOException {

    StringWriter writer = new StringWriter();
    JsonGenerator gen  = jsonFactory.createGenerator(writer);

    aliasChanges.writeJson(gen);
    gen.flush();
    return writer.toString();
  }

  public void createIndexes() throws IOException {

    List<? extends SpiBeanType<?>> beanTypes = server.getBeanTypes();
    for (SpiBeanType<?> beanType : beanTypes) {

      if (beanType.isDocStoreMapped()) {
        String mappingJson = mappingsBuilder.createMappingJson(beanType);
        String name = beanType.getDocStoreIndexName();
        createIndexWithMapping(name+"_v1", name, mappingJson);
      }

    }
  }

}
