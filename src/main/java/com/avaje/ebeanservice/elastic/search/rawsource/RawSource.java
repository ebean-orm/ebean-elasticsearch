package com.avaje.ebeanservice.elastic.search.rawsource;

import java.util.Map;

/**
 * Raw search result hit.
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

  /**
   * Return the source document as a Map.
   */
  public Map<String, Object> getSource() {
    return source;
  }

  /**
   * Return the Id value.
   */
  public String getId() {
    return id;
  }

  /**
   * Return the score.
   */
  public double getScore() {
    return score;
  }

  /**
   * Return the index name.
   */
  public String getIndex() {
    return index;
  }

  /**
   * Return the index type.
   */
  public String getType() {
    return type;
  }
}