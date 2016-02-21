package com.avaje.ebeanservice.elastic.support;

import java.io.IOException;
import java.util.Set;

/**
 * Sends the JSON to the ElasticSearch Bulk API.
 */
public interface IndexMessageSender {

  /**
   * Send the JSON to the ElasticSearch Bulk API.
   */
  String postBulk(String json) throws IOException;

  /**
   * Get the document source for a specific document.
   */
  IndexMessageResponse getDocSource(String indexType, String indexName, String docId) throws IOException;

  IndexMessageResponse postQuery(boolean scroll, String indexType, String indexName, String jsonQuery) throws IOException;

  IndexMessageResponse getScroll(String scrollId) throws IOException;

  IndexMessageResponse clearScrollIds(Set<String> scrollIds) throws IOException;
}
