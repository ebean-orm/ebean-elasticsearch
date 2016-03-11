package com.avaje.ebeanservice.elastic.querywriter;

import com.avaje.ebean.search.TextCommonTerms;
import com.fasterxml.jackson.core.JsonGenerator;

import java.io.IOException;

/**
 * Writes MATCH expressions as Elastic JSON.
 */
class WriteCommonTermsExpression extends WriteBase {

  WriteCommonTermsExpression() {
  }

  /**
   * Write the match expression.
   */
  void write(JsonGenerator json, String value, TextCommonTerms options) throws IOException {

    json.writeStartObject();
    json.writeObjectFieldStart("common");
    json.writeObjectFieldStart("body");
    json.writeStringField("query", value);

    writeCutoffFrequency(json, options.getCutoffFrequency());
    if (options.isLowFreqOperatorAnd()) {
      json.writeStringField("low_freq_operator", "and");
    }
    if (options.isHighFreqOperatorAnd()) {
      json.writeStringField("high_freq_operator", "and");
    }

    if (!writeMinShouldMatch(json, options.getMinShouldMatch())) {
      if (has(options.getMinShouldMatchLowFreq()) || has(options.getMinShouldMatchHighFreq())) {
        json.writeObjectFieldStart("minimum_should_match");
        if (has(options.getMinShouldMatchLowFreq())) {
          json.writeStringField("low_freq", options.getMinShouldMatchLowFreq());
        }
        if (has(options.getMinShouldMatchHighFreq())) {
          json.writeStringField("high_freq", options.getMinShouldMatchHighFreq());
        }
        json.writeEndObject();
      }
    }

    json.writeEndObject();
    json.writeEndObject();
    json.writeEndObject();
  }

}
