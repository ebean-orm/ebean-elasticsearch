package com.avaje.ebeanservice.elastic.query;

import com.avaje.ebean.QueryEachConsumer;
import com.avaje.ebean.QueryEachWhileConsumer;
import com.avaje.ebean.text.json.JsonContext;
import com.avaje.ebeanservice.docstore.api.DocQueryRequest;
import com.avaje.ebeanservice.elastic.search.bean.BeanSearchParser;
import com.fasterxml.jackson.core.JsonParser;

import java.io.IOException;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 * Processes Query findEach/findEachWhile requests.
 */
public class EQueryEach<T> extends EQuery<T> {

  private final DocQueryRequest<T> request;

  private final EQuerySend send;

  private final Set<String> allScrollIds = new LinkedHashSet<String>();

  private BeanSearchParser<T> beanParser;

  private String currentScrollId;

  public EQueryEach(DocQueryRequest<T> request, EQuerySend send, JsonContext jsonContext) {
    super(request.getQuery(), jsonContext, request.createJsonReadOptions());
    this.send = send;
    this.request = request;
  }

  /**
   * Return true if all the hits are read (scroll query with only 1 scroll).
   */
  private boolean allHitsRead() {
    return beanParser.allHitsRead();
  }

  /**
   * Return true if there were no hits (so end of scroll).
   */
  private boolean zeroHits() {
    return beanParser.zeroHits();
  }

  /**
   * Perform the initial scroll query.
   */
  private List<T> fetchInitial() throws IOException {
    JsonParser initialJson = send.findScroll(beanType.docStore(), query);
    beanParser = createParser(initialJson);
    return read();
  }

  /**
   * Perform a subsequent scroll query.
   */
  private List<T> fetchNextScroll() throws IOException {
    JsonParser moreJson = send.findNextScroll(currentScrollId);
    beanParser = beanParser.moreJson(moreJson, true);
    return read();
  }

  /**
   * Read and return the hits also collecting the scrollId (for later clearing).
   */
  private List<T> read() throws IOException {

    List<T> hits = beanParser.read();
    currentScrollId = beanParser.getScrollId();
    allScrollIds.add(currentScrollId);
    return hits;
  }

  /**
   * Clear all the scroll ids from ElasticSearch.
   */
  public void clearScrollIds() {
    send.clearScrollIds(allScrollIds);
  }

  /**
   * Consume the initial scroll returning true if there are more.
   */
  public boolean consumeInitial(QueryEachConsumer<T> consumer) throws IOException {
    List<T> list = fetchInitial();
    request.executeSecondaryQueries(true);
    consumeEach(list, consumer);
    return !allHitsRead();
  }

  /**
   * Consume subsequent scroll returning true if there are more.
   */
  public boolean consumeMore(QueryEachConsumer<T> consumer) throws IOException {
    List<T> list = fetchNextScroll();
    request.executeSecondaryQueries(true);
    consumeEach(list, consumer);
    return !zeroHits();
  }

  private void consumeEach(List<T> moreList, QueryEachConsumer<T> consumer) {
    for (T bean : moreList) {
      consumer.accept(bean);
    }
  }

  /**
   * Consume the initial scroll returning true if we should continue.
   */
  public boolean consumeInitialWhile(QueryEachWhileConsumer<T> consumer) throws IOException {
    return consumeWhile(fetchInitial(), consumer) && !allHitsRead();
  }

  /**
   * Consume a subsequent scroll returning true if we should continue.
   */
  public boolean consumeMoreWhile(QueryEachWhileConsumer<T> consumer) throws IOException {
    return consumeWhile(fetchNextScroll(), consumer) && !zeroHits();
  }

  private boolean consumeWhile(List<T> moreList, QueryEachWhileConsumer<T> consumer) {
    for (T bean : moreList) {
      if (!consumer.accept(bean)) {
        return false;
      }
    }
    return true;
  }
}
