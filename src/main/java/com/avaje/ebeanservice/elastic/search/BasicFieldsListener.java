package com.avaje.ebeanservice.elastic.search;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 *
 */
public class BasicFieldsListener implements SearchFieldsListener {

  final List<SearchRow> rows = new ArrayList<SearchRow>();

  @Override
  public void process(Map<String, Object> fields, String id, double score, String index, String type) {

    rows.add(new SearchRow(fields, id, score, index, type));
  }

  public List<SearchRow> getRows() {
    return rows;
  }
}
