package com.avaje.ebeanservice.elastic.querywriter;

import com.avaje.ebean.search.Match;
import com.avaje.ebean.text.json.JsonContext;
import com.fasterxml.jackson.core.JsonGenerator;

import java.io.IOException;
import java.io.StringWriter;

/**
 * Context for helping write JSON.
 */
public class ElasticJsonContext {

  private final JsonContext jsonContext;

  private final MatchWriter matchWriter;

  public ElasticJsonContext(JsonContext jsonContext) {
    this.jsonContext = jsonContext;
    this.matchWriter = new MatchWriter(jsonContext);
  }

  /**
   * Create a new JsonGenerator.
   */
  public JsonGenerator createGenerator(StringWriter writer) {
    return jsonContext.createGenerator(writer);
  }

  /**
   * Write a scalar value (handles any type known to Ebean - Enums, Java8, Joda etc).
   */
  public void writeScalar(JsonGenerator json, Object value) throws IOException {
    jsonContext.writeScalar(json, value);
  }

  /**
   * Write a match expression.
   */
  public void writeMatch(JsonGenerator json, String propertyName, String value, Match options) throws IOException {
    matchWriter.writeMatch(json, propertyName, value, options);
  }
}
