package io.ebeanservice.elastic.index;

import com.fasterxml.jackson.core.JsonFactory;
import io.ebean.DB;
import io.ebean.plugin.BeanType;
import io.ebean.plugin.SpiServer;
import org.example.domain.Country;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;

public class EIndexMappingsBuilderTest {

  @Test
  public void createMappingJson() {

    // we don't need the docstore active to run this test
    System.setProperty("ebean.docstore.active","false");

    EIndexMappingsBuilder mappingsBuilder = new EIndexMappingsBuilder(new JsonFactory());

    SpiServer server = DB.getDefault().pluginApi();
    BeanType<Country> countryType = server.beanType(Country.class);

    String mappingJson = mappingsBuilder.createMappingJson(countryType);

    assertEquals(mappingJson, "{\n" +
        "  \"mappings\" : {\n" +
        "    \"properties\" : {\n" +
        "      \"name\": { \"type\": \"text\" }\n" +
        "    }\n" +
        "  }\n" +
        "}");
  }
}