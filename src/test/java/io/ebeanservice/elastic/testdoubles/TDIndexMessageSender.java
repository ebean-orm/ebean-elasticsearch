package io.ebeanservice.elastic.testdoubles;

import io.ebeanservice.elastic.support.IndexMessageResponse;
import io.ebeanservice.elastic.support.IndexMessageSender;

import java.io.IOException;
import java.util.Set;

/**
 */
public class TDIndexMessageSender implements IndexMessageSender {

  public String request;
  public String response = "{\"something\":42}";

  public TDIndexMessageSender() {
  }

  @Override
  public String postBulk(String json) throws IOException {
    request = json;
    return response;
  }

  @Override
  public IndexMessageResponse getDocSource(String indexName, String docId) throws IOException {
    return null;
  }

  @Override
  public IndexMessageResponse postQuery(boolean scroll, String indexName, String jsonQuery) throws IOException {
    return null;
  }

  @Override
  public IndexMessageResponse postUpdateQuery(String indexType, String indexName, String jsonQuery) throws IOException {
    return null;
  }

  @Override
  public IndexMessageResponse getScroll(String scrollId) throws IOException {
    return null;
  }

  @Override
  public IndexMessageResponse clearScrollIds(Set<String> scrollIds) throws IOException {
    return null;
  }

  @Override
  public void indexSettings(String indexName, String settingsJson) throws IOException {

  }

  @Override
  public boolean indexExists(String indexName) throws IOException {
    return false;
  }

  @Override
  public boolean indexDelete(String indexName) throws IOException {
    return false;
  }

  @Override
  public void indexCreate(String indexName, String settingsJson) throws IOException {

  }

  @Override
  public void indexAlias(String aliasJson) throws IOException {

  }
}
