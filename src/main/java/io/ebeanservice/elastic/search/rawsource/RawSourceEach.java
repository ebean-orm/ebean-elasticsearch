package io.ebeanservice.elastic.search.rawsource;

import io.ebeanservice.docstore.api.RawDoc;
import io.ebeanservice.elastic.query.EConsumeWhile;
import io.ebeanservice.elastic.query.EQuerySend;
import com.fasterxml.jackson.core.JsonParser;

import java.io.IOException;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Predicate;

/**
 * Process scroll query with RawSource.
 */
public class RawSourceEach implements EConsumeWhile<RawDoc> {

  private final EQuerySend send;
  private final String indexName;
  private final String jsonQuery;

  private final Set<String> scrollIds = new LinkedHashSet<>();

  private long totalCount;

  private RawSourceReader currentReader;

  private String currentScrollId;

  public RawSourceEach(EQuerySend send, String indexName, String jsonQuery) {
    this.send = send;
    this.indexName = indexName;
    this.jsonQuery = jsonQuery;
  }

  private List<RawDoc> fetchInitial() throws IOException {
    return read(send.findScroll(indexName, jsonQuery));
  }

  private List<RawDoc> fetchNextScroll() throws IOException {
    return read(send.findNextScroll(currentScrollId));
  }

  /**
   * Consume initial scroll results returning true if we should continue.
   */
  public boolean consumeInitial(Consumer<RawDoc> consumer) throws IOException {
    consume(consumer, fetchInitial());
    return !currentReader.allHitsRead();
  }

  /**
   * Consume next scroll and return true if we should continue.
   */
  public boolean consumeNext(Consumer<RawDoc> consumer) throws IOException {
    consume(consumer, fetchNextScroll());
    return !currentReader.zeroHits();
  }

  private void consume(Consumer<RawDoc> consumer, List<RawDoc> list) {
    for (RawDoc bean : list) {
      totalCount++;
      consumer.accept(bean);
    }
  }

  /**
   * Consume the initial scroll returning true if we should continue.
   */
  public boolean consumeInitialWhile(Predicate<RawDoc> consumer) throws IOException {
    List<RawDoc> docs = fetchInitial();
    return consumeWhile(docs, consumer) && !docs.isEmpty();
  }


  /**
   * Consume a subsequent scroll returning true if we should continue.
   */
  public boolean consumeMoreWhile(Predicate<RawDoc> consumer) throws IOException {
    List<RawDoc> docs = fetchNextScroll();
    return consumeWhile(docs, consumer) && !docs.isEmpty();
  }


  private boolean consumeWhile(List<RawDoc> moreList, Predicate<RawDoc> consumer) {
    for (RawDoc bean : moreList) {
      if (!consumer.test(bean)) {
        return false;
      }
    }
    return true;
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
  private List<RawDoc> read(JsonParser json) throws IOException {
    currentReader = new RawSourceReader(json);
    return readInternal();
  }

  /**
   * Read the JSON response including collecting the scrollId (for later clearing).
   */
  private List<RawDoc> readInternal() throws IOException {

    List<RawDoc> hits = currentReader.read();
    currentScrollId = currentReader.getScrollId();
    scrollIds.add(currentScrollId);
    return hits;
  }

}
