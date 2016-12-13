package io.ebeanservice.elastic.bulk;

import io.ebean.config.JsonConfig;
import io.ebean.text.json.EJson;
import io.ebeanservice.elastic.ElasticDocumentStore;
import io.ebeanservice.elastic.support.IndexMessageSender;
import io.ebeanservice.elastic.support.StringBuilderWriter;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import org.slf4j.Logger;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;

/**
 * Sends Bulk API messages to ElasticSearch.
 */
public class BulkSender {

  private static Logger bulkLogger = ElasticDocumentStore.BULK;

  private final JsonFactory jsonFactory;

  private final JsonConfig.Include defaultInclude;

  private final Object defaultObjectMapper;

  private final IndexMessageSender messageSender;

  /**
   * Construct with appropriate JSON configuration.
   */
  public BulkSender(JsonFactory jsonFactory, JsonConfig.Include defaultInclude, Object defaultObjectMapper, IndexMessageSender messageSender) {
    this.jsonFactory = jsonFactory;
    this.defaultInclude = defaultInclude;
    this.defaultObjectMapper = defaultObjectMapper;
    this.messageSender = messageSender;
  }

  /**
   * Create a new buffer with appropriate JSON setup.
   */
  public BulkBuffer newBuffer() throws IOException {

    StringBuilderWriter writer = new StringBuilderWriter(500);
    JsonGenerator gen = jsonFactory.createGenerator(writer);
    return new BulkBuffer(gen, writer, defaultObjectMapper, defaultInclude);
  }

  /**
   * Send the bulk buffer returning the response.
   */
  public Map<String, Object> sendBulk(BulkBuffer buffer) throws IOException {

    buffer.flush();

    String content = buffer.getContent();
    if (content.isEmpty()) {
      if (bulkLogger.isDebugEnabled()) {
        bulkLogger.debug("ElasticBulkMessage is empty?");
      }
      return Collections.emptyMap();
    }

    if (bulkLogger.isTraceEnabled()) {
      bulkLogger.trace("ElasticBulkMessage Request:\n{}", content);
    }
    String response = messageSender.postBulk(content);
    if (bulkLogger.isTraceEnabled()) {
      bulkLogger.trace("ElasticBulkMessage Response:\n{}", response);
    }

    return parseBulkResponse(response);
  }

  /**
   * Parse the returned JSON response into a Map.
   */
  private Map<String, Object> parseBulkResponse(String response) throws IOException {
    return EJson.parseObject(response);
  }

}
