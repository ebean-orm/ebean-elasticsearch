package com.avaje.ebeanservice.elastic.index;

import com.avaje.ebean.plugin.SpiBeanType;
import com.avaje.ebean.plugin.SpiServer;
import com.fasterxml.jackson.core.JsonFactory;

import java.util.List;

/**
 *
 */
public class IndexService {

  final SpiServer server;

  final JsonFactory jsonFactory;

  final IndexMappingsBuilder mappingsBuilder;


  public IndexService(SpiServer server, JsonFactory jsonFactory) {
    this.server = server;
    this.jsonFactory = jsonFactory;
    this.mappingsBuilder = new IndexMappingsBuilder(jsonFactory);
  }

  public void createIndexes() {

    List<? extends SpiBeanType<?>> beanTypes = server.getBeanTypes();
    for (SpiBeanType<?> beanType : beanTypes) {

      if (beanType.isDocStoreMapped()) {
        String mappingJson = mappingsBuilder.createMappingJson(beanType);
      }

    }
  }

}
