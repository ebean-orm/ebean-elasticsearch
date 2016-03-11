package com.avaje.ebeanservice.elastic.querywriter;

import com.avaje.ebean.search.Match;
import com.avaje.ebean.search.MultiMatch;
import com.avaje.ebean.search.TextCommonTerms;
import com.avaje.ebean.search.TextQueryString;
import com.avaje.ebean.search.TextSimple;
import com.avaje.ebean.text.json.JsonContext;
import com.fasterxml.jackson.core.JsonGenerator;

import java.io.IOException;
import java.io.StringWriter;

/**
 * Context for helping write JSON expressions.
 */
public class ElasticJsonContext {

  private final JsonContext jsonContext;

  private final WriteMatchExpression matchWriter;

  private final WriteCommonTermsExpression commonTermsWriter;

  private final WriteTextSimpleExpression simpleWriter;

  private final WriteQueryStringExpression queryStringWriter;

  /**
   * Construct with the Ebean JsonContext (which handles all scalar types know to Ebean).
   */
  public ElasticJsonContext(JsonContext jsonContext) {
    this.jsonContext = jsonContext;
    this.matchWriter = new WriteMatchExpression(jsonContext);
    this.commonTermsWriter = new WriteCommonTermsExpression();
    this.simpleWriter = new WriteTextSimpleExpression();
    this.queryStringWriter = new WriteQueryStringExpression();
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

  /**
   * Write a multi-match expression.
   */
  public void writeMultiMatch(JsonGenerator json, String search, MultiMatch options) throws IOException {
    matchWriter.writeMultiMatch(json, search, options);
  }

  /**
   * Write a common terms expression.
   */
  public void writeCommonTerms(JsonGenerator json, String search, TextCommonTerms options) throws IOException {
    commonTermsWriter.write(json, search, options);
  }

  /**
   * Write a query string expression.
   */
  public void writeQueryString(JsonGenerator json, String search, TextQueryString options) throws IOException {
    queryStringWriter.write(json, search, options);
  }

  /**
   * Write a simple query expression.
   */
  public void writeSimple(JsonGenerator json, String search, TextSimple options) throws IOException {
    simpleWriter.write(json, search, options);
  }
}
