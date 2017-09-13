package io.ebeanservice.elastic.bulk;

import io.ebean.config.JsonConfig;
import io.ebean.FetchPath;
import io.ebeaninternal.api.SpiEbeanServer;
import io.ebeaninternal.server.text.json.WriteJson;
import io.ebeanservice.docstore.api.DocStoreUpdateContext;
import com.fasterxml.jackson.core.JsonGenerator;

import java.io.IOException;
import java.io.Writer;

/**
 * For ElasticSearch Bulk API processing this holds the JsonGenerator and associated data.
 * <p>
 *   This is used to build requests to be sent to the ElasticSearch Bulk API.
 * </p>
 */
public class BulkBuffer implements DocStoreUpdateContext {

  private final JsonGenerator generator;

  private final Writer writer;

  private final Object defaultObjectMapper;

  private final JsonConfig.Include defaultInclude;

  public BulkBuffer(JsonGenerator generator, Writer writer, Object defaultObjectMapper, JsonConfig.Include defaultInclude) {
    this.generator = generator;
    this.writer = writer;
    this.defaultObjectMapper = defaultObjectMapper;
    this.defaultInclude = defaultInclude;
  }

  public WriteJson createWriteJson(SpiEbeanServer server, JsonGenerator gen, FetchPath fetchPath) {
    return new WriteJson(server, gen, fetchPath, null, defaultObjectMapper, defaultInclude);
  }

  /**
   * Return the buffer content (Bulk API JSON with new lines etc).
   */
  public String getContent() {
    return writer.toString();
  }

  /**
   * Return the JsonGenerator to write the JSON content to.
   */
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
