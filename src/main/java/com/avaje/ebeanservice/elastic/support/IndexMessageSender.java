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

  /**
   * Send a query request.
   */
  IndexMessageResponse postQuery(boolean scroll, String indexType, String indexName, String jsonQuery) throws IOException;

  /**
   * Send an update query request.
   */
  IndexMessageResponse postUpdateQuery(String indexType, String indexName, String jsonQuery) throws IOException;

  /**
   * Send a get scroll request.
   */
  IndexMessageResponse getScroll(String scrollId) throws IOException;

  /**
   * Send a clearScrollIds request.
   */
  IndexMessageResponse clearScrollIds(Set<String> scrollIds) throws IOException;

  /**
   * Send an indexExists request.
   */
  boolean indexExists(String indexName) throws IOException;

  /**
   * Send a delete index request.
   */
  boolean indexDelete(String indexName) throws IOException;

  /**
   * Send a create index request.
   */
  void indexCreate(String indexName, String settingsJson) throws IOException;

  /**
   * Send a create index alias request.
   */
  void indexAlias(String aliasJson) throws IOException;

  /**
   * Set the settings on the index.
   */
  void indexSettings(String indexName, String settingsJson) throws IOException;
}
