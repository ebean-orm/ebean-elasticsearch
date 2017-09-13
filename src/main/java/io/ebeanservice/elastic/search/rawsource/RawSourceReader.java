package io.ebeanservice.elastic.search.rawsource;

import io.ebean.text.json.EJson;
import io.ebeanservice.docstore.api.RawDoc;
import io.ebeanservice.elastic.search.BaseSearchResultParser;
import com.fasterxml.jackson.core.JsonParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Reads JSON response parsing it into a list of RawSource objects.
 */
public class RawSourceReader extends BaseSearchResultParser {

  private final List<RawDoc> list = new ArrayList<>();

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
    list.add(new RawDoc(source, id, score, index, type));
  }

  @Override
  public void readFields() throws IOException {
    // do nothing, expect to only read source
  }

  @Override
  public void readIdOnly() {
    list.add(new RawDoc(null, id, score, index, type));
  }

  public List<RawDoc> read() throws IOException {
    readAll();
    return list;
  }
}
