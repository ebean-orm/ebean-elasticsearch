package com.avaje.ebeanservice.elastic;

import com.avaje.ebean.config.JsonConfig;
import com.avaje.ebean.FetchPath;
import com.avaje.ebeaninternal.api.SpiEbeanServer;
import com.avaje.ebeaninternal.server.text.json.WriteJson;
import com.avaje.ebeanservice.docstore.api.DocStoreUpdateContext;
import com.fasterxml.jackson.core.JsonGenerator;

import java.io.IOException;
import java.io.Writer;

/**
 * For ElasticSearch Bulk API processing this holds the JsonGenerator and associated data.
 * <p>
 *   This is used to build requests to be sent to the ElasticSearch Bulk API.
 * </p>
 */
public class ElasticBulkUpdate implements DocStoreUpdateContext {

  private final JsonGenerator generator;

  private final Writer writer;

  private final Object defaultObjectMapper;

  private final JsonConfig.Include defaultInclude;

  public ElasticBulkUpdate(JsonGenerator generator, Writer writer, Object defaultObjectMapper, JsonConfig.Include defaultInclude) {
    this.generator = generator;
    this.writer = writer;
    this.defaultObjectMapper = defaultObjectMapper;
    this.defaultInclude = defaultInclude;
  }

  //@Override
  public WriteJson createWriteJson(SpiEbeanServer server, JsonGenerator gen, FetchPath fetchPath) {
    return new WriteJson(server, gen, fetchPath, null, defaultObjectMapper, defaultInclude);
  }

  public String getBuffer() {
    return writer.toString();
  }

  /**
   * Return the JsonGenerator to write the JSON content to.
   */
  //@Override
  public JsonGenerator gen() {
    return generator;
  }

  /**
   * Flush and close.
   */
  public void flush() throws IOException {
    generator.flush();
    generator.close();
  }
}
