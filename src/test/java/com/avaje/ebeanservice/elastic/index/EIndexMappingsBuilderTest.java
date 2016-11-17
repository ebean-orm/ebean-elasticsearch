package com.avaje.ebeanservice.elastic.index;

import com.avaje.ebean.Ebean;
import com.avaje.ebean.plugin.BeanType;
import com.avaje.ebean.plugin.SpiServer;
import com.fasterxml.jackson.core.JsonFactory;
import org.example.domain.Country;
import org.testng.annotations.Test;

import static org.testng.Assert.*;

public class EIndexMappingsBuilderTest {

  @Test
  public void createMappingJson() throws Exception {

    // we don't need the docstore active to run this test
    System.setProperty("ebean.docstore.active","false");

    EIndexMappingsBuilder mappingsBuilder = new EIndexMappingsBuilder(new JsonFactory());

    SpiServer server = Ebean.getDefaultServer().getPluginApi();
    BeanType<Country> countryType = server.getBeanType(Country.class);

    String mappingJson = mappingsBuilder.createMappingJson(countryType);

    assertEquals(mappingJson, "{\n" +
        "  \"mappings\" : {\n" +
        "    \"country\" : {\n" +
        "      \"properties\" : {\n" +
        "        \"name\": { \"type\": \"text\" }\n" +
        "      }\n" +
        "    }\n" +
        "  }\n" +
        "}");
  }
}