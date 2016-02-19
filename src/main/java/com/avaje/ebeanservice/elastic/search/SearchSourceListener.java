package com.avaje.ebeanservice.elastic.search;

import com.fasterxml.jackson.core.JsonParser;

import java.io.IOException;
import java.util.Map;

/**
 */
public interface SearchSourceListener {

  void readSource(JsonParser parser, String id) throws IOException;

  void readFields(Map<String, Object> fields, String id, double score);

}
