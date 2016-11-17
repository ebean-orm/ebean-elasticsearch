package com.avaje.ebeanservice.elastic.querywriter;

import com.avaje.ebean.search.TextQueryString;
import com.fasterxml.jackson.core.JsonGenerator;

import java.io.IOException;

/**
 * Base writer methods full text query expression.
 */
abstract class WriteBase {

  protected void writeCutoffFrequency(JsonGenerator json, double cutoffFreq) throws IOException {
    if (cutoffFreq != 0) {
      json.writeNumberField("cutoff_frequency", cutoffFreq);
    }
  }

  protected boolean writeMinShouldMatch(JsonGenerator json, String minShouldMatch) throws IOException {
    if (has(minShouldMatch)) {
      json.writeStringField("minimum_should_match", minShouldMatch);
      return true;
    }
    return false;
  }

  protected void writeMaxExpansions(JsonGenerator json, int maxExpansions) throws IOException {
    if (maxExpansions != 0) {
      json.writeNumberField("max_expansions", maxExpansions);
    }
  }

  protected void writeOperator(JsonGenerator json, boolean operatorAnd) throws IOException {
    if (operatorAnd) {
      json.writeStringField("operator", "and");
    }
  }

  protected void writeTieBreaker(JsonGenerator json, double tieBreaker) throws IOException {
    if (tieBreaker != 0) {
      json.writeNumberField("tie_breaker", tieBreaker);
    }
  }

  protected void writeBoost(JsonGenerator json, double boost) throws IOException {
    if (boost != 0) {
      json.writeNumberField("boost", boost);
    }
  }

  protected void writeRewrite(JsonGenerator json, String rewrite) throws IOException {
    if (has(rewrite)) {
      json.writeStringField("rewrite", rewrite);
    }
  }

  protected void writeFuzziness(JsonGenerator json, String fuzziness) throws IOException {
    if (has(fuzziness)) {
      json.writeStringField("fuzziness", fuzziness);
    }
  }

  protected void writeAnalyzer(JsonGenerator json, String analyzer) throws IOException {
    if (has(analyzer)) {
      json.writeStringField("analyzer", analyzer);
    }
  }

  protected void writeZeroTerms(JsonGenerator json, String zeroTerms) throws IOException {
    if (has(zeroTerms)) {
      json.writeStringField("zero_terms_query", zeroTerms);
    }
  }

  protected void writeFields(JsonGenerator json, String[] fields) throws IOException {
    if (fields != null && fields.length > 0) {
      json.writeArrayFieldStart("fields");
      for (String field : fields) {
        json.writeString(field);
      }
      json.writeEndArray();
    }
  }

  protected void writeLenient(JsonGenerator json, boolean lenient) throws IOException {
    if (lenient) {
      json.writeBooleanField("lenient", true);
    }
  }

  protected void writeLocale(JsonGenerator json, String locale) throws IOException {
    if (has(locale)) {
      json.writeStringField("locale", locale);
    }
  }

  protected void writeAnalyzeWildcard(JsonGenerator json, boolean analyzeWildcard) throws IOException {
    if (analyzeWildcard) {
      json.writeBooleanField("analyze_wildcard", true);
    }
  }

  protected void writeLowerCaseExpandedTerms(JsonGenerator json, boolean lowercaseExpandedTerms) throws IOException {
    if (!lowercaseExpandedTerms) {
      json.writeBooleanField("lowercase_expanded_terms", false);
    }
  }

  protected void writeFlags(JsonGenerator json, String flags) throws IOException {
    if (has(flags)) {
      json.writeStringField("flags", flags);
    }
  }

  protected void writeUseDisMax(JsonGenerator json, boolean useDisMax) throws IOException {
    if (!useDisMax) {
      json.writeBooleanField("use_dis_max", false);
    }
  }

  protected void writeTimeZone(JsonGenerator json, String timeZone) throws IOException {
    if (has(timeZone)) {
      json.writeStringField("time_zone", timeZone);
    }
  }

  protected void writeAutoGeneratePhraseQueries(JsonGenerator json, boolean autoGeneratePhraseQueries) throws IOException {
    if (autoGeneratePhraseQueries) {
      json.writeBooleanField("auto_generate_phrase_queries", autoGeneratePhraseQueries);
    }
  }

  protected void writePhraseSlop(JsonGenerator json, double phraseSlop) throws IOException {
    if (phraseSlop != 0) {
      json.writeNumberField("phrase_slop", phraseSlop);
    }
  }

  protected void writeFuzzyPrefixLength(JsonGenerator json, int fuzzyPrefixLength) throws IOException {
    if (fuzzyPrefixLength != 0) {
      json.writeNumberField("fuzzy_prefix_length", fuzzyPrefixLength);
    }
  }

  protected void writeFuzzyMatchExpansions(JsonGenerator json, int fuzzyMaxExpansions) throws IOException {
    if (fuzzyMaxExpansions != TextQueryString.DEFAULT_FUZZY_MAX_EXPANSIONS) {
      json.writeNumberField("fuzzy_max_expansions", fuzzyMaxExpansions);
    }
  }

  protected void writeEnablePositionIncrements(JsonGenerator json, boolean enablePositionIncrements) throws IOException {
    if (!enablePositionIncrements) {
      json.writeBooleanField("enable_position_increments", false);
    }
  }

  protected void writeAllowLeadingWildcard(JsonGenerator json, boolean allowLeadingWildcard) throws IOException {
    if (!allowLeadingWildcard) {
      json.writeBooleanField("allow_leading_wildcard", false);
    }
  }

  protected void writeDefaultOperator(JsonGenerator json, boolean operatorAnd) throws IOException {
    if (operatorAnd) {
      json.writeStringField("default_operator", "and");
    }
  }

  protected void writeDefaultField(JsonGenerator json, String defaultField) throws IOException {
    if (has(defaultField)) {
      json.writeStringField("default_field", defaultField);
    }
  }

  protected boolean has(String value) {
    return value != null && !value.trim().isEmpty();
  }

}
