package com.avaje.ebeanservice.elastic.search;

import com.avaje.ebean.text.json.EJson;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;

import java.io.IOException;
import java.util.Map;

/**
 */
public class SearchResultParser {

  //final SearchFieldsListener fieldsListener;
  final SearchSourceListener sourceListener;

  final JsonParser parser;
  int documentLevel;

  long took;
  boolean timedOut;
  Map<String, Object> shards;

  String field;
  long total;
  double maxScore;

  String index;
  String type;
  String id;
  double score;
  Map<String, Object> fields;

  public SearchResultParser(JsonParser parser, SearchSourceListener sourceListener) {
    this.parser = parser;
    //this.fieldsListener = fieldsListener;
    this.sourceListener = sourceListener;
  }

  public void read() throws IOException {

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
    sourceListener.readSource(parser, id);
  }

  private void readFields() throws IOException {
    fields = EJson.parseObject(parser);
    sourceListener.readFields(fields, id, score);
    //fieldsListener.process(fields, id, score, index, type);
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
