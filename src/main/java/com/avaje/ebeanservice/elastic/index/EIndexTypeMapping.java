package com.avaje.ebeanservice.elastic.index;

import com.avaje.ebeanservice.docstore.api.mapping.DocPropertyType;

import java.util.HashMap;
import java.util.Map;

/**
 * Mapping of general document store types to ElasticSearch types.
 */
public class EIndexTypeMapping {

  final Map<DocPropertyType,String> map = new HashMap<DocPropertyType,String>();

  public EIndexTypeMapping() {
    map.put(DocPropertyType.ENUM,"keyword");
    map.put(DocPropertyType.UUID,"keyword");
    map.put(DocPropertyType.KEYWORD,"keyword");
    map.put(DocPropertyType.TEXT,"text");
    map.put(DocPropertyType.BOOLEAN,"boolean");

    map.put(DocPropertyType.SHORT,"short");
    map.put(DocPropertyType.INTEGER,"integer");
    map.put(DocPropertyType.LONG,"long");
    map.put(DocPropertyType.DOUBLE,"double");
    map.put(DocPropertyType.FLOAT,"float");

    map.put(DocPropertyType.DATE,"date");
    map.put(DocPropertyType.DATETIME,"date");

    map.put(DocPropertyType.BINARY,"binary");

    map.put(DocPropertyType.LIST,"nested");
    map.put(DocPropertyType.OBJECT,"object");
    map.put(DocPropertyType.ROOT,"root");

  }

  /**
   * Return the ElasticSearch type given the general doc store type.
   */
  public String get(DocPropertyType type) {
    return map.get(type);
  }
}
