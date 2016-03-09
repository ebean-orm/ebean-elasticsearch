package com.avaje.ebeanservice.elastic.querywriter;

import com.avaje.ebean.search.Match;
import com.avaje.ebean.text.json.JsonContext;
import com.fasterxml.jackson.core.JsonGenerator;

import java.io.IOException;

/**
 * Writes MATCH expressions as Elastic JSON.
 */
class MatchWriter {

  private static final String MATCH = "match";

  private final JsonContext jsonContext;

  MatchWriter(JsonContext jsonContext) {
    this.jsonContext = jsonContext;
  }

  /**
   * Write the match expression.
   */
  void writeMatch(JsonGenerator json, String propertyName, String value, Match options) throws IOException {

    json.writeStartObject();
    json.writeObjectFieldStart(MATCH);
    json.writeFieldName(propertyName);
    if (options == null) {
      jsonContext.writeScalar(json, value);
    } else {
      json.writeStartObject();
      json.writeFieldName("query");
      jsonContext.writeScalar(json, value);
      if (options.isAnd()) {
        json.writeStringField("operator", "and");
      }
      if (options.getBoost() != 0) {
        json.writeNumberField("boost", options.getBoost());
      }
      if (options.getCutoffFrequency() != 0) {
        json.writeNumberField("cutoff_frequency", options.getCutoffFrequency());
      }
      if (has(options.getMinShouldMatch())) {
        json.writeStringField("minimum_should_match", options.getMinShouldMatch());
      }
      if (has(options.getZeroTerms())) {
        json.writeStringField("zero_terms_query", options.getZeroTerms());
      }
      if (has(options.getAnalyzer())) {
        json.writeStringField("analyzer", options.getAnalyzer());
      }
      if (options.isPhrasePrefix()) {
        json.writeStringField("type", "phrase_prefix");
        if (options.getMaxExpansions() > 0) {
          json.writeNumberField("max_expansions", options.getMaxExpansions());
        }

      } else if (options.isPhrase()) {
        json.writeStringField("type", "phrase");
      }
      json.writeEndObject();
    }
    json.writeEndObject();
    json.writeEndObject();
  }

  private boolean has(String value) {
    return value != null && !value.trim().isEmpty();
  }

}
