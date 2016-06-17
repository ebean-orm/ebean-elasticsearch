package com.avaje.ebeanservice.elastic.index;

import com.avaje.ebean.PersistenceIOException;
import com.avaje.ebean.config.DocStoreConfig;
import com.avaje.ebean.plugin.BeanType;
import com.avaje.ebean.plugin.SpiServer;
import com.avaje.ebean.text.json.EJson;
import com.avaje.ebeanservice.elastic.support.IndexMessageSender;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.LineNumberReader;
import java.io.StringWriter;
import java.util.Map;

/**
 * Index exists, drop, create functions.
 */
public class EIndexService {

  private static final Logger logger = LoggerFactory.getLogger(EIndexService.class);

  private final SpiServer server;

  private final DocStoreConfig config;

  private final JsonFactory jsonFactory;

  private final EIndexMappingsBuilder mappingsBuilder;

  private final IndexMessageSender sender;

  private final boolean generateMapping;

  private final boolean createIndexes;

  private final boolean dropCreateIndexes;

  public EIndexService(SpiServer server, JsonFactory jsonFactory, IndexMessageSender sender) {
    this.server = server;
    this.jsonFactory = jsonFactory;
    this.sender = sender;
    this.config = server.getServerConfig().getDocStoreConfig();
    this.mappingsBuilder = new EIndexMappingsBuilder(jsonFactory);
    this.generateMapping = config.isGenerateMapping();
    this.dropCreateIndexes = config.isDropCreate();
    this.createIndexes = config.isCreate() || dropCreateIndexes;
  }

  /**
   * Return true if the index exists.
   */
  public boolean indexExists(String indexName) throws IOException {
    return sender.indexExists(indexName);
  }

  /**
   * Drop the index.
   */
  public void dropIndex(String indexName) throws IOException {
    sender.indexDelete(indexName);
  }

  /**
   * Set the settings on the index.
   */
  public void indexSettings(String indexName, Map<String, Object> settings) throws IOException {

    StringWriter writer = new StringWriter();
    EJson.write(settings, writer);

    String settingsJson = "{\"index\":" + writer.toString() + "}";
    sender.indexSettings(indexName, settingsJson);
  }

  /**
   * Create the index reading the mapping from the expected resource location.
   */
  public void createIndex(String indexName, String alias) throws IOException {

    String resourcePath = indexResourcePath(indexName);
    String rawJsonMapping = readResource(resourcePath);
    if (rawJsonMapping == null) {
      throw new IllegalArgumentException("No resource " + resourcePath + " found in classPath");
    }

    if (!createIndexWithMapping(false, indexName, alias, rawJsonMapping)) {
      throw new IllegalArgumentException("Index " + indexName + " not created as it already exists?");
    }
  }

  private String indexResourcePath(String indexName) {
    return "/" + getMappingPath() + "/" + indexName + getMappingSuffix();
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
      String line;
      while ((line = lineReader.readLine()) != null) {
        buffer.append(line);
      }

      return buffer.toString();

    } catch (IOException e) {
      throw new PersistenceIOException(e);
    }
  }

  /**
   * Create an index given the mapping.
   *
   * @param dropCreate If true drop the index prior to creating it.
   * @param indexName The name of the index (typically suffixed with a version number).
   * @param alias The optional alias name. If not null creates the alias for the index.
   * @param jsonMapping The mapping for the index.
   *
   * @return True if the index was created or false if it already exists and dropCreate was false.
   */
  public boolean createIndexWithMapping(boolean dropCreate, String indexName, String alias, String jsonMapping) throws IOException {

    if (indexExists(indexName)) {
      if (!dropCreate) {
        logger.debug("index {} already exists", indexName);
        return false;
      }
      logger.debug("drop index {}", indexName);
      dropIndex(indexName);
    }
    logger.debug("create index {}", indexName);
    sender.indexCreate(indexName, jsonMapping);
    if (alias != null) {
      if (indexExists(alias)) {
        logger.debug("drop alias {}", alias, indexName);
        dropIndex(alias);
      }
    }
    if (alias != null) {
      String aliasJson = asJson(new AliasChanges().add(indexName, alias));
      logger.debug("add alias {} for index {}", alias, indexName);
      sender.indexAlias(aliasJson);
    }
    return true;
  }

  private String asJson(AliasChanges aliasChanges) throws IOException {

    StringWriter writer = new StringWriter();
    JsonGenerator gen = jsonFactory.createGenerator(writer);

    aliasChanges.writeJson(gen);
    gen.flush();
    return writer.toString();
  }

  /**
   * On startup generate mappings and create indexes as per configuration options.
   */
  public void createIndexesOnStartup() throws IOException {
    if (generateMapping || createIndexes) {
      for (BeanType<?> beanType : server.getBeanTypes()) {
        if (beanType.isDocStoreMapped()) {
          createIndex(beanType);
        }
      }
    }
  }

  private void createIndex(BeanType<?> beanType) throws IOException {

    String alias = beanType.docStore().getIndexName();
    // hardcode _v1 suffix until there is some plan for handling 'index migrations'
    String indexName = alias + "_v1";

    String mappingJson = null;
    if (generateMapping) {
      mappingJson = mappingsBuilder.createMappingJson(beanType);
      writeMappingFile(indexName, mappingJson);
    }

    if (createIndexes) {
      if (mappingJson == null) {
        String mappingPath = indexResourcePath(indexName);
        mappingJson = readResource(mappingPath);
        if (mappingJson == null) {
          throw new IllegalArgumentException("No resource " + mappingPath + " found in classPath");
        }
      }
      createIndexWithMapping(dropCreateIndexes, indexName, alias, mappingJson);
    }
  }

  /**
   * Write the mapping to a file.
   */
  private void writeMappingFile(String indexName, String mappingJson) {

    File resourceDir = new File(config.getPathToResources());
    if (!resourceDir.exists()) {
      logger.error("docStore.pathToResources [{}] does not exist?", config.getPathToResources());
      return;
    }
    try {
      String mappingPath = getMappingPath();
      File dir = new File(resourceDir, mappingPath);
      if (!dir.exists() && !dir.mkdirs()) {
        logger.warn("Unable to make directories for {}", dir.getAbsolutePath());
      }
      String mappingSuffix = getMappingSuffix();
      File file = new File(dir, indexName + mappingSuffix);
      FileWriter writer = new FileWriter(file);
      writer.write(mappingJson);
      writer.flush();
      writer.close();
    } catch (IOException e) {
      logger.error("Error trying to write index mapping", e);
    }
  }

  private String getMappingSuffix() {
    String mappingSuffix = config.getMappingSuffix();
    if (mappingSuffix == null) {
      mappingSuffix = ".mapping.json";
    }
    return mappingSuffix;
  }

  private String getMappingPath() {

    String mappingPath = config.getMappingPath();
    if (mappingPath == null) {
      mappingPath = "elastic-mapping";
    }
    return mappingPath;
  }

}
