package com.avaje.ebeanservice.elastic.search;

import java.util.Map;

/**
 */
public interface SearchFieldsListener {

  void process(Map<String, Object> rowFields, String rowId, double rowScore, String rowIndex, String rowIndexType);
}
