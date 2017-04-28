package com.avaje.ebeanservice.elastic.query;

import com.avaje.ebean.plugin.BeanDocType;
import com.avaje.ebean.text.json.JsonContext;
import com.avaje.ebeaninternal.api.SpiQuery;
import com.avaje.ebeanservice.docstore.api.DocumentNotFoundException;
import com.avaje.ebeanservice.elastic.querywriter.ElasticJsonContext;
import com.avaje.ebeanservice.elastic.querywriter.ElasticDocQueryContext;
import com.avaje.ebeanservice.elastic.support.IndexMessageResponse;
import com.avaje.ebeanservice.elastic.support.IndexMessageSender;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
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

  private final ElasticJsonContext elasticJsonContext;

  public EQuerySend(JsonContext jsonContext, JsonFactory jsonFactory, IndexMessageSender messageSender) {
    this.jsonFactory = jsonFactory;
    this.messageSender = messageSender;
    this.elasticJsonContext = new ElasticJsonContext(jsonContext);
  }

  /**
   * Execute as find hits returning the resulting JSON response.
   */
  public <T> JsonParser findHits(BeanDocType<T> type, SpiQuery<T> query) throws IOException, DocumentNotFoundException {
    return findInternal(false, type, query);
  }

  /**
   * Execute as find scroll returning the resulting JSON response.
   */
  public <T> JsonParser findScroll(BeanDocType<T> type, SpiQuery<T> query) throws IOException, DocumentNotFoundException {
    return findInternal(true, type, query);
  }

  private <T> JsonParser findInternal(boolean scroll, BeanDocType<T> type, SpiQuery<T> query) throws IOException, DocumentNotFoundException {

    IndexMessageResponse response = messageSender.postQuery(scroll, type.getIndexType(), type.getIndexName(), asJson(query));
    switch (response.getCode()) {
      case 404:
        throw new DocumentNotFoundException("404 for query?");
      case 200:
        return jsonFactory.createParser(response.getBody());
      default:
        throw new IOException("Unhandled response code " + response.getCode() + " body:" + response.getBody());
    }
  }

  /**
   * Return the query as ElasticSearch JSON format.
   */
  private <T> String asJson(SpiQuery<T> query) {
    return ElasticDocQueryContext.asJson(elasticJsonContext, query);
  }

  /**
   * Execute Get by Id returning the JSON response.
   */
  public JsonParser findById(String indexType, String indexName, Object docId) throws IOException, DocumentNotFoundException {

    IndexMessageResponse response = messageSender.getDocSource(indexType, indexName, docId.toString());
    switch (response.getCode()) {
      case 404:
        throw new DocumentNotFoundException("404 for docId:" + docId);
      case 200:
        return jsonFactory.createParser(response.getBody());
      default:
        throw new IOException("Unhandled response code " + response.getCode() + " body:" + response.getBody());
    }
  }

  /**
   * Execute find next scroll returning the JSON response.
   */
  public JsonParser findNextScroll(String scrollId) throws IOException, DocumentNotFoundException {

    IndexMessageResponse response = messageSender.getScroll(scrollId);
    switch (response.getCode()) {
      case 404:
        throw new DocumentNotFoundException("404 for scrollId:" + scrollId);
      case 200:
        return jsonFactory.createParser(response.getBody());
      default:
        throw new IOException("Unhandled response code " + response.getCode() + " body:" + response.getBody());
    }
  }

  /**
   * Clear all the scroll Ids.
   */
  public void clearScrollIds(Set<String> scrollIds) {
    try {
      messageSender.clearScrollIds(scrollIds);
    } catch (IOException e) {
      logger.error("Error trying to clear scrollIds: {}", scrollIds, e);
    }
  }

}
