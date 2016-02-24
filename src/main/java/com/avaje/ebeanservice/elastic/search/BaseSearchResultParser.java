package com.avaje.ebeanservice.elastic.search;

import com.avaje.ebean.text.json.EJson;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;

import java.io.IOException;
import java.util.Map;

/**
 * Base parser for reading/processing search results.
 */
public abstract class BaseSearchResultParser {

  protected final JsonParser parser;

  protected int documentLevel;

  protected long took;
  protected boolean timedOut;
  protected Map<String, Object> shards;
  protected String scrollId;

  protected String field;
  protected long total;
  protected double maxScore;

  protected String index;
  protected String type;
  protected String id;
  protected double score;

  public BaseSearchResultParser(JsonParser parser) {
    this.parser = parser;
  }

  /**
   * Return the scrollId.
   */
  public String getScrollId() {
    return scrollId;
  }

  /**
   * Return true if all the hits have been read.
   */
  public abstract boolean allHitsRead();

  /**
   * Return true if the total hits is zero.
   */
  public abstract boolean zeroHits();

  /**
   * Return the JSON returning the list of beans.
   */
  public void readAll() throws IOException {

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
  protected boolean nextFieldName() throws IOException {
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

  protected void readLevel2() throws IOException {
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

  public abstract void readSource() throws IOException;

  public abstract void readFields() throws IOException;

  protected void readLevel1() throws IOException {
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

  protected void readLevel0() throws IOException {

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

  protected String readString() throws IOException {
    parser.nextToken();
    return parser.getValueAsString();
  }

  protected long readLong() throws IOException {
    parser.nextToken();
    return parser.getLongValue();
  }

  protected double readDouble() throws IOException {
    JsonToken token = parser.nextToken();
    if (token == JsonToken.VALUE_NULL) {
      return 0;
    } else {
      return parser.getDoubleValue();
    }
  }

  protected boolean readBoolean() throws IOException {
    parser.nextToken();
    return parser.getBooleanValue();
  }
}
