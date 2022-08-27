package io.ebeanservice.elastic.bulk;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import io.ebean.config.JsonConfig;
import io.ebean.text.json.EJson;
import io.ebeanservice.elastic.ElasticDocumentStore;
import io.ebeanservice.elastic.support.IndexMessageResponse;
import io.ebeanservice.elastic.support.IndexMessageSender;
import io.ebeanservice.elastic.support.StringBuilderWriter;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;

import static java.lang.System.Logger.Level.DEBUG;
import static java.lang.System.Logger.Level.TRACE;

/**
 * Sends Bulk API messages to ElasticSearch.
 */
public class BulkSender {

  private static final System.Logger bulkLogger = ElasticDocumentStore.BULK;

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
      if (bulkLogger.isLoggable(DEBUG)) {
        bulkLogger.log(DEBUG, "ElasticBulkMessage is empty?");
      }
      return Collections.emptyMap();
    }

    if (bulkLogger.isLoggable(TRACE)) {
      bulkLogger.log(TRACE, "ElasticBulkMessage Request:\n{0}", content);
    }
    String response = messageSender.postBulk(content);
    if (bulkLogger.isLoggable(TRACE)) {
      bulkLogger.log(TRACE, "ElasticBulkMessage Response:\n{0}", response);
    }

    return parseBulkResponse(response);
  }

  /**
   * Parse the returned JSON response into a Map.
   */
  private Map<String, Object> parseBulkResponse(String response) throws IOException {
    return EJson.parseObject(response);
  }

  public Map<String, Object> setUpdateQuery(String indexName, String indexType, String script) throws IOException {
    IndexMessageResponse response = messageSender.postUpdateQuery(indexName, indexType, script);
    return EJson.parseObject(response.getBody());
  }
}
