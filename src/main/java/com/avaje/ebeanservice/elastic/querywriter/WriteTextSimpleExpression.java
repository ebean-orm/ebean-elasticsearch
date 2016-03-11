package com.avaje.ebeanservice.elastic.querywriter;

import com.avaje.ebean.search.TextSimple;
import com.fasterxml.jackson.core.JsonGenerator;

import java.io.IOException;

/**
 * Write the text simple expression.
 */
class WriteTextSimpleExpression extends WriteBase {

  WriteTextSimpleExpression() {
  }

  void write(JsonGenerator json, String value, TextSimple options) throws IOException {

    json.writeStartObject();
    json.writeObjectFieldStart("simple_query_string");
    json.writeStringField("query", value);


    writeAnalyzer(json, options.getAnalyzer());
    writeFields(json, options.getFields());
    if (options.isOperatorAnd()) {
      json.writeStringField("default_operator", "and");
    }
    writeFlags(json, options.getFlags());
    writeLowerCaseExpandedTerms(json, options.isLowercaseExpandedTerms());
    writeAnalyzeWildcard(json, options.isAnalyzeWildcard());
    writeLocale(json, options.getLocale());
    writeLenient(json, options.isLenient());
    writeMinShouldMatch(json, options.getMinShouldMatch());


    json.writeEndObject();
    json.writeEndObject();
  }

}
