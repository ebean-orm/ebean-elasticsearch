package com.avaje.ebeanservice.elastic.querywriter;

import com.avaje.ebean.search.TextQueryString;
import com.fasterxml.jackson.core.JsonGenerator;

import java.io.IOException;

/**
 * Writes query string query expression.
 */
class WriteQueryStringExpression extends WriteBase {

  WriteQueryStringExpression() {
  }

  void write(JsonGenerator json, String value, TextQueryString options) throws IOException {

    json.writeStartObject();
    json.writeObjectFieldStart("query_string");
    json.writeStringField("query", value);


    writeFields(json, options.getFields());
    writeDefaultField(json, options.getDefaultField());
    writeDefaultOperator(json, options.isOperatorAnd());
    writeAnalyzer(json, options.getAnalyzer());
    writeAllowLeadingWildcard(json, options.isAllowLeadingWildcard());
    writeLowerCaseExpandedTerms(json, options.isLowercaseExpandedTerms());
    writeFuzzyMatchExpansions(json, options.getFuzzyMaxExpansions());
    writeFuzziness(json, options.getFuzziness());
    writeFuzzyPrefixLength(json, options.getFuzzyPrefixLength());
    writePhraseSlop(json, options.getPhraseSlop());
    writeBoost(json, options.getBoost());
    writeAnalyzeWildcard(json, options.isAnalyzeWildcard());
    writeAutoGeneratePhraseQueries(json, options.isAutoGeneratePhraseQueries());
    writeMinShouldMatch(json, options.getMinShouldMatch());
    writeLenient(json, options.isLenient());
    writeLocale(json, options.getLocale());
    writeTimeZone(json, options.getTimeZone());
    writeUseDisMax(json, options.isUseDisMax());
    writeTieBreaker(json, options.getTieBreaker());
    writeRewrite(json, options.getRewrite());

    json.writeEndObject();
    json.writeEndObject();
  }

}
