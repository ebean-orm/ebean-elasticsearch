package com.avaje.ebeanservice.elastic.query;

import com.avaje.ebean.plugin.BeanDocType;
import com.avaje.ebean.text.json.JsonContext;
import com.avaje.ebeaninternal.api.SpiQuery;
import com.avaje.ebeanservice.docstore.api.DocumentNotFoundException;
import com.avaje.ebeanservice.elastic.support.ElasticQueryContext;
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

  private final JsonContext jsonContext;

  private final JsonFactory jsonFactory;

  private final IndexMessageSender messageSender;

  public EQuerySend(JsonContext jsonContext, JsonFactory jsonFactory, IndexMessageSender messageSender) {
    this.jsonContext = jsonContext;
    this.jsonFactory = jsonFactory;
    this.messageSender = messageSender;
  }

  public JsonParser findHits(BeanDocType type, SpiQuery<?> query) throws IOException, DocumentNotFoundException {
    return findInternal(false, type, query);
  }

  public JsonParser findScroll(BeanDocType type, SpiQuery<?> query) throws IOException, DocumentNotFoundException {
    return findInternal(true, type, query);
  }

  private JsonParser findInternal(boolean scroll, BeanDocType type, SpiQuery<?> query) throws IOException, DocumentNotFoundException {

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
  private String asJson(SpiQuery<?> query) {
    return ElasticQueryContext.asJson(jsonContext, query);
  }

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


  public void clearScrollIds(Set<String> scrollIds) {
    try {
      messageSender.clearScrollIds(scrollIds);
    } catch (IOException e) {
      logger.error("Error trying to clear scrollIds: " + scrollIds, e);
    }
  }

}
