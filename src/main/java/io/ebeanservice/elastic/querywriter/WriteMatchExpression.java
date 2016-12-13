package io.ebeanservice.elastic.querywriter;

import io.ebean.search.AbstractMatch;
import io.ebean.search.Match;
import io.ebean.search.MultiMatch;
import io.ebean.text.json.JsonContext;
import com.fasterxml.jackson.core.JsonGenerator;

import java.io.IOException;

/**
 * Writes MATCH expressions as Elastic JSON.
 */
class WriteMatchExpression extends WriteBase {

  private static final String MATCH = "match";

  private static final String MULTI_MATCH = "multi_match";

  private final JsonContext jsonContext;

  WriteMatchExpression(JsonContext jsonContext) {
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
        writeMaxExpansions(json, options.getMaxExpansions());

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
    writeTieBreaker(json, options.getTieBreaker());
    writeMaxExpansions(json, options.getMaxExpansions());
    writeBaseOptions(json, options);

    json.writeEndObject();
    json.writeEndObject();
  }


  private void writeBaseOptions(JsonGenerator json, AbstractMatch options) throws IOException {

    writeOperator(json, options.isOperatorAnd());
    writeBoost(json, options.getBoost());
    writeCutoffFrequency(json, options.getCutoffFrequency());
    writeMinShouldMatch(json, options.getMinShouldMatch());
    writeZeroTerms(json, options.getZeroTerms());
    writeAnalyzer(json, options.getAnalyzer());
    writeFuzziness(json, options.getFuzziness());
    writeRewrite(json, options.getRewrite());
  }

}
