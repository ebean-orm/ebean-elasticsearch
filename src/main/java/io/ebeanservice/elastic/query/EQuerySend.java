package io.ebeanservice.elastic.query;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import io.ebeanservice.docstore.api.DocumentNotFoundException;
import io.ebeanservice.elastic.support.IndexMessageResponse;
import io.ebeanservice.elastic.support.IndexMessageSender;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Set;

/**
 * Query request sender.
 */
public class EQuerySend {

  private static final Logger logger = LoggerFactory.getLogger(EQuerySend.class);

  private final JsonFactory jsonFactory;

  private final IndexMessageSender messageSender;

  EQuerySend(JsonFactory jsonFactory, IndexMessageSender messageSender) {
    this.jsonFactory = jsonFactory;
    this.messageSender = messageSender;
  }

  /**
   * Execute as find hits returning the resulting JSON response.
   */
  JsonParser findHits(String indexName, String jsonQuery) throws IOException {
    return findInternal(false, indexName, jsonQuery);
  }

  /**
   * Execute as find scroll returning the resulting JSON response.
   */
  public JsonParser findScroll(String indexName, String jsonQuery) throws IOException {
    return findInternal(true, indexName, jsonQuery);
  }

  private JsonParser findInternal(boolean scroll, String indexName, String jsonQuery) throws IOException {

    IndexMessageResponse response = messageSender.postQuery(scroll, indexName, jsonQuery);
    switch (response.getCode()) {
      case 404:
        throw new DocumentNotFoundException("404 for query?");
      case 200:
        return jsonFactory.createParser(response.getBody());
      default:
        throw new IOException(unhandled(response));
    }
  }

  private String unhandled(IndexMessageResponse response) {
    return "Unhandled response code " + response.getCode() + " body:" + response.getBody();
  }

  /**
   * Execute Get by Id returning the JSON response.
   */
  JsonParser findById(String nameType, Object docId) throws IOException {

    IndexMessageResponse response = messageSender.getDocSource(nameType, docId.toString());
    switch (response.getCode()) {
      case 404:
        throw new DocumentNotFoundException("404 for docId:" + docId);
      case 200:
        return jsonFactory.createParser(response.getBody());
      default:
        throw new IOException(unhandled(response));
    }
  }

  /**
   * Execute find next scroll returning the JSON response.
   */
  public JsonParser findNextScroll(String scrollId) throws IOException {

    IndexMessageResponse response = messageSender.getScroll(scrollId);
    switch (response.getCode()) {
      case 404:
        throw new DocumentNotFoundException("404 for scrollId:" + scrollId);
      case 200:
        return jsonFactory.createParser(response.getBody());
      default:
        throw new IOException(unhandled(response));
    }
  }

  /**
   * Clear all the scroll Ids.
   */
  public void clearScrollIds(Set<String> scrollIds) {
    try {
      messageSender.clearScrollIds(scrollIds);
    } catch (IOException e) {
      logger.error("Error trying to clear scrollIds: " + scrollIds, e);
    }
  }

}
