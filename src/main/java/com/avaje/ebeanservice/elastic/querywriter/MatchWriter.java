package com.avaje.ebeanservice.elastic.querywriter;

import com.avaje.ebean.search.BaseMatch;
import com.avaje.ebean.search.Match;
import com.avaje.ebean.search.MultiMatch;
import com.avaje.ebean.text.json.JsonContext;
import com.fasterxml.jackson.core.JsonGenerator;

import java.io.IOException;

/**
 * Writes MATCH expressions as Elastic JSON.
 */
class MatchWriter {

  private static final String MATCH = "match";

  private static final String MULTI_MATCH = "multi_match";

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
      writeBaseOptions(json, options);
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

  /**
   * Write the multi-match expression.
   */
  public void writeMultiMatch(JsonGenerator json, String search, MultiMatch options) throws IOException {

    json.writeStartObject();
    json.writeObjectFieldStart(MULTI_MATCH);
    json.writeStringField("query", search);

    String[] fields = options.getFields();

    json.writeArrayFieldStart("fields");
    for (String field : fields) {
      json.writeString(field);
    }
    json.writeEndArray();
    if (options.getType() != MultiMatch.Type.BEST_FIELDS) {
      json.writeStringField("type", options.getType().name().toLowerCase());
    }
    if (options.getTieBreaker() != 0) {
      json.writeNumberField("tie_breaker", options.getTieBreaker());
    }
    if (options.getMaxExpansions() > 0) {
      json.writeNumberField("max_expansions", options.getMaxExpansions());
    }
    writeBaseOptions(json, options);
    json.writeEndObject();
    json.writeEndObject();
  }

  private void writeBaseOptions(JsonGenerator json, BaseMatch options) throws IOException {

    if (options.isAnd()) {
      json.writeStringField("operator", "and");
    }
    if (options.getBoost() != 0) {
      json.writeNumberField("boost", options.getBoost());
    }
    if (options.getCutoffFrequency() != 0) {
      json.writeNumberField("cutoff_frequency", options.getCutoffFrequency());
    }
    if (options.getMaxExpansions() != 0) {
      json.writeNumberField("max_expansions", options.getMaxExpansions());
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
    if (has(options.getFuzziness())) {
      json.writeStringField("fuzziness", options.getFuzziness());
    }
    if (has(options.getRewrite())) {
      json.writeStringField("rewrite", options.getRewrite());
    }
  }

  private boolean has(String value) {
    return value != null && !value.trim().isEmpty();
  }

}
