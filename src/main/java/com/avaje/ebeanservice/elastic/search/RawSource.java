package com.avaje.ebeanservice.elastic.search;

import java.util.Map;

/**
 * Created by rob on 24/02/16.
 */
public class RawSource {

  private final Map<String, Object> source;
  private final String id;
  private final double score;
  private final String index;
  private final String type;

  public RawSource(Map<String, Object> source, String id, double score, String index, String type) {
    this.source = source;
    this.id = id;
    this.score = score;
    this.index = index;
    this.type = type;
  }

  public Map<String, Object> getSource() {
    return source;
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