package com.avaje.ebeanservice.elastic.support;

import java.io.IOException;

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

  IndexMessageResponse postQuery(String indexType, String indexName, String jsonQuery) throws IOException;
}
