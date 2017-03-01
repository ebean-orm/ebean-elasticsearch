package io.ebeanservice.elastic.search.rawsource;

import io.ebean.plugin.BeanDocType;
import io.ebeaninternal.api.SpiQuery;
import io.ebeanservice.elastic.query.EQuerySend;
import com.fasterxml.jackson.core.JsonParser;

import java.io.IOException;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Consumer;

/**
 * Process scroll query with RawSource.
 */
public class RawSourceEach {

  private final EQuerySend send;
  private final String nameType;
  private final String jsonQuery;

  private final Set<String> scrollIds = new LinkedHashSet<String>();

  private long totalCount;

  private RawSourceReader currentReader;

  private String currentScrollId;

  public RawSourceEach(EQuerySend send, String nameType, String jsonQuery) {
    this.send = send;
    this.nameType = nameType;
    this.jsonQuery = jsonQuery;
  }

  /**
   * Consume initial scroll results returning true if we should continue.
   */
  public boolean consumeInitial(Consumer<RawSource> consumer) throws IOException {

    JsonParser json = send.findScroll(nameType, jsonQuery);
    consume(consumer, read(json));
    return !currentReader.allHitsRead();
  }

  /**
   * Consume next scroll and return true if we should continue.
   */
  public boolean consumeNext(Consumer<RawSource> consumer) throws IOException {

    JsonParser moreJson = send.findNextScroll(currentScrollId);
    consume(consumer, read(moreJson));
    return !currentReader.zeroHits();
  }

  private void consume(Consumer<RawSource> consumer, List<RawSource> list) {
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
