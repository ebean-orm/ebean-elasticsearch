package com.avaje.ebeanservice.elastic.search;

import java.util.Map;

/**
 *
 */
public class SearchRow {

  final Map<String, Object> fields;

  final String id;
  final double score;
  final String index;
  final String type;


  public SearchRow(Map<String, Object> fields, String id, double score, String index, String type) {
    this.fields = fields;
    this.id = id;
    this.score = score;
    this.index = index;
    this.type = type;
  }

  public Map<String, Object> getFields() {
    return fields;
  }

  public String getId() {
    return id;
  }

  public double getScore() {
    return score;
  }

  public String getIndex() {
    return index;
  }

  public String getType() {
    return type;
  }
}
