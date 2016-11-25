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
  private static final String TODAY = "$today";
  private static final String LAST_DAYS = "$last-";

  private final JsonFactory jsonFactory;

  private final IndexMessageSender messageSender;

  private final ElasticJsonContext elasticJsonContext;

  EQuerySend(JsonContext jsonContext, JsonFactory jsonFactory, IndexMessageSender messageSender) {
    this.jsonFactory = jsonFactory;
    this.messageSender = messageSender;
    this.elasticJsonContext = new ElasticJsonContext(jsonContext);
  }

  /**
   * Execute as find hits returning the resulting JSON response.
   */
  JsonParser findHits(BeanDocType type, SpiQuery<?> query) throws IOException {
    return findInternal(false, type, query);
  }

  /**
   * Execute as find scroll returning the resulting JSON response.
   */
  public JsonParser findScroll(BeanDocType type, SpiQuery<?> query) throws IOException {
    return findInternal(true, type, query);
  }

  private JsonParser findInternal(boolean scroll, BeanDocType type, SpiQuery<?> query) throws IOException {

    String docIndexName = query.getDocIndexName();
    String nameType;
    if (docIndexName != null) {
      nameType = nameType(docIndexName, type);
    } else {
      nameType = indexNameType(type);
    }

    IndexMessageResponse response = messageSender.postQuery(scroll, nameType, asJson(query));
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

  private String indexNameType(BeanDocType type) {
    return type.getIndexName() + "/" + type.getIndexType();
  }

  /**
   * Return the index name and type taking into account $today and $last-3 etc.
   */
  private String nameType(String docIndexName, BeanDocType type) {
    if (TODAY.equals(docIndexName)) {
      return type.getIndexName()+"2016.11.20" + "/" + type.getIndexType();

    } else if (docIndexName.startsWith(LAST_DAYS)) {
      return type.getIndexName()+"2016.11.21" + "/" + type.getIndexType();
    }
    return docIndexName;
  }

  /**
   * Return the query as ElasticSearch JSON format.
   */
  private String asJson(SpiQuery<?> query) {
    return ElasticDocQueryContext.asJson(elasticJsonContext, query);
  }

  /**
   * Execute Get by Id returning the JSON response.
   */
  JsonParser findById(BeanDocType beanDocType, Object docId) throws IOException {

    IndexMessageResponse response = messageSender.getDocSource(indexNameType(beanDocType), docId.toString());
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
