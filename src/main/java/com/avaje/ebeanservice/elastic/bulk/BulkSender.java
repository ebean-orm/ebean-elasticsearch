package com.avaje.ebeanservice.elastic.bulk;

import com.avaje.ebean.config.JsonConfig;
import com.avaje.ebean.text.json.EJson;
import com.avaje.ebeanservice.elastic.ElasticDocumentStore;
import com.avaje.ebeanservice.elastic.support.IndexMessageSender;
import com.avaje.ebeanservice.elastic.support.StringBuilderWriter;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
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

  public BulkSender(JsonFactory jsonFactory, JsonConfig.Include defaultInclude, Object defaultObjectMapper, IndexMessageSender messageSender) {
    this.jsonFactory = jsonFactory;
    this.defaultInclude = defaultInclude;
    this.defaultObjectMapper = defaultObjectMapper;
    this.messageSender = messageSender;
  }

  /**
   * Create a new BulkBuffer with appropriate JSON setup.
   */
  public BulkBuffer newBuffer() throws IOException {

    StringBuilderWriter writer = new StringBuilderWriter();
    JsonGenerator gen = jsonFactory.createGenerator(writer);
    return new BulkBuffer(gen, writer, defaultObjectMapper, defaultInclude);
  }

  /**
   * Send the bulk buffer returning the response.
   */
  public Map<String, Object> sendBulk(BulkBuffer bulk) throws IOException {

    bulk.flush();

    String payload = bulk.getBuffer();
    if (bulkLogger.isTraceEnabled()) {
      bulkLogger.trace("ElasticBulkMessage Request:\n{}", payload);
    }

    String response = messageSender.postBulk(payload);

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
