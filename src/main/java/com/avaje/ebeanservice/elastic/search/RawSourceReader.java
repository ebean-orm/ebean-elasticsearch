package com.avaje.ebeanservice.elastic.search;

import com.avaje.ebean.text.json.EJson;
import com.fasterxml.jackson.core.JsonParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 *
 */
public class RawSourceReader extends BaseSearchResultParser {

  private final List<RawSource> list = new ArrayList<RawSource>();

  public RawSourceReader(JsonParser parser) {
    super(parser);
  }

  @Override
  public boolean allHitsRead() {
    return total == 0 || total == list.size();
  }

  @Override
  public boolean zeroHits() {
    return list.isEmpty();
  }

  @Override
  public void readSource() throws IOException {
    Map<String, Object> source = EJson.parseObject(parser);
    list.add(new RawSource(source, id, score, index, type));
  }

  @Override
  public void readFields() throws IOException {
    // do nothing, expect to only read source
  }

  public List<RawSource> read() throws IOException {
    readAll();
    return list;
  }
}
