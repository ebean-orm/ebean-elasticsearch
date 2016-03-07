package com.avaje.ebeanservice.elastic.search.rawsource;

import com.avaje.ebean.Query;
import com.avaje.ebean.QueryEachConsumer;
import com.avaje.ebean.plugin.BeanDocType;
import com.avaje.ebeanservice.elastic.query.EQuerySend;
import com.fasterxml.jackson.core.JsonParser;

import java.io.IOException;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 * Process scroll query with RawSource.
 */
public class RawSourceEach {

  private final EQuerySend send;

  private final Set<String> scrollIds = new LinkedHashSet<String>();

  private long totalCount;

  private RawSourceReader currentReader;

  private String currentScrollId;

  public RawSourceEach(EQuerySend send) {
    this.send = send;
  }

  /**
   * Consume initial scroll results returning true if we should continue.
   */
  public boolean consumeInitial(QueryEachConsumer<RawSource> consumer, BeanDocType beanDocType, Query<?> query) throws IOException {

    JsonParser json = send.findScroll(beanDocType, query);
    consume(consumer, read(json));
    return !currentReader.allHitsRead();
  }

  /**
   * Consume next scroll and return true if we should continue.
   */
  public boolean consumeNext(QueryEachConsumer<RawSource> consumer) throws IOException {

    JsonParser moreJson = send.findNextScroll(currentScrollId);
    consume(consumer, read(moreJson));
    return !currentReader.zeroHits();
  }

  private void consume(QueryEachConsumer<RawSource> consumer, List<RawSource> list) {
    for (RawSource bean : list) {
      totalCount++;
      consumer.accept(bean);
    }
  }

  /**
   * Clear the scrollIds on the server.
   */
  public void clearScrollIds() {
    send.clearScrollIds(scrollIds);
  }

  /**
   * Return the total count of documents processed.
   */
  public long getTotalCount() {
    return totalCount;
  }

  /**
   * Read the JSON results as list of RawSource.
   */
  private List<RawSource> read(JsonParser json) throws IOException {

    currentReader = new RawSourceReader(json);
    return readInternal();
  }

  /**
   * Read the JSON response including collecting the scrollId (for later clearing).
   */
  private List<RawSource> readInternal() throws IOException {

    List<RawSource> hits = currentReader.read();
    currentScrollId = currentReader.getScrollId();
    scrollIds.add(currentScrollId);

    return hits;
  }

}
