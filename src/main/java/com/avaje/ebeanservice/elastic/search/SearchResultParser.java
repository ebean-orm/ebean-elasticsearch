package com.avaje.ebeanservice.elastic.search;

import com.avaje.ebean.plugin.SpiBeanType;
import com.avaje.ebean.text.json.EJson;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 */
public class SearchResultParser<T> {

  final BeanSourceListener<T> listener;

  final JsonParser parser;

  int documentLevel;

  long took;
  boolean timedOut;
  Map<String, Object> shards;
  String scrollId;

  String field;
  long total;
  double maxScore;

  String index;
  String type;
  String id;
  double score;
  Map<String, Object> fields;

  public SearchResultParser(JsonParser parser, SpiBeanType<T> desc) {
    this.parser = parser;
    this.listener = new BeanSourceListener<T>(desc);
  }

  public String getScrollId() {
    return scrollId;
  }

  /**
   * Return true if all the hits have been read.
   */
  public boolean allHitsRead() {
    return total == 0 || total == listener.size();
  }

  /**
   * Return true if the total hits is zero.
   */
  public boolean zeroHits() {
    return listener.size() == 0;
  }

  /**
   * Return the JSON returning the list of beans.
   */
  public List<T> read() throws IOException {

    parser.nextToken();
    while (nextFieldName()) {
      field = parser.getCurrentName();
      switch (documentLevel) {
        case 0:
          readLevel0();
          break;
        case 1:
          readLevel1();
          break;
        case 2:
          readLevel2();
          break;
        default:
          throw new IllegalStateException("Unexpected documentLevel "+ documentLevel);
      }
    }

    return listener.getList();
  }

  /**
   * Move forward to the next field name if possible.
   */
  private boolean nextFieldName() throws IOException {
    JsonToken token = parser.nextToken();
    while (true) {
      switch (token) {
        case FIELD_NAME:
          return true;
        case END_ARRAY:
          return false;
        case END_OBJECT:
          token = parser.nextToken();
          break;
        case START_ARRAY:
          token = parser.nextToken();
          break;
        case START_OBJECT:
          token = parser.nextToken();
          break;
      }
    }
  }

  private void readLevel2() throws IOException {
    if ("_index".equals(field)) {
      index = readString();
    } else if ("_type".equals(field)) {
      type = readString();
    } else if ("_id".equals(field)) {
      id = readString();
    } else if ("_score".equals(field)) {
      score = readDouble();
    } else if ("fields".equals(field)) {
      readFields();
    } else if ("_source".equals(field)) {
      readSource();
    } else {
      throw new IllegalStateException("Unrecognized field at level 2: '" + field + "'!");
    }
  }

  private void readSource() throws IOException {
    listener.readSource(parser, id);
  }

  private void readFields() throws IOException {
    fields = EJson.parseObject(parser);
    listener.readFields(fields, id, score);
  }

  private void readLevel1() throws IOException {
    if ("total".equals(field)) {
      total = readLong();
    } else if ("max_score".equals(field)) {
      maxScore = readDouble();
    } else if ("hits".equals(field)) {
      // read array start and then object start
      parser.nextToken();
      //parser.nextToken();
      documentLevel = 2;
    } else {
      throw new IllegalStateException("Unrecognized field at level 1: '" + field + "'!");
    }
  }

  private void readLevel0() throws IOException {

    if ("took".equals(field)) {
      took = readLong();
    } else if ("timed_out".equals(field)) {
      timedOut = readBoolean();
    } else if ("_shards".equals(field)) {
      shards = EJson.parseObject(parser);
    } else if ("_scroll_id".equals(field)) {
      scrollId = readString();
    } else if ("hits".equals(field)) {
      // read object start
      parser.nextToken();
      documentLevel = 1;
    } else {
      throw new IllegalStateException("Unrecognized field at level 0: '" + field + "'!");
    }
  }

  private String readString() throws IOException {
    parser.nextToken();
    return parser.getValueAsString();
  }

  private long readLong() throws IOException {
    parser.nextToken();
    return parser.getLongValue();
  }

  private double readDouble() throws IOException {
    JsonToken token = parser.nextToken();
    if (token == JsonToken.VALUE_NULL) {
      return 0;
    } else {
      return parser.getDoubleValue();
    }
  }

  private boolean readBoolean() throws IOException {
    parser.nextToken();
    return parser.getBooleanValue();
  }
}
