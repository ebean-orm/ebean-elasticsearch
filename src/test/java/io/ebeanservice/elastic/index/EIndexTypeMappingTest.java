package io.ebeanservice.elastic.index;

import io.ebeanservice.docstore.api.mapping.DocPropertyType;
import org.testng.annotations.Test;

import static org.testng.Assert.*;

public class EIndexTypeMappingTest {

  EIndexTypeMapping mapping = new EIndexTypeMapping();

  @Test
  public void get_with_various() throws Exception {

    assertEquals(mapping.get(DocPropertyType.UUID), "keyword");
    assertEquals(mapping.get(DocPropertyType.ENUM), "keyword");
    assertEquals(mapping.get(DocPropertyType.KEYWORD), "keyword");
    assertEquals(mapping.get(DocPropertyType.TEXT), "text");
    assertEquals(mapping.get(DocPropertyType.BINARY), "binary");

    assertEquals(mapping.get(DocPropertyType.DATE), "date");
    assertEquals(mapping.get(DocPropertyType.DATETIME), "date");

    assertEquals(mapping.get(DocPropertyType.LIST), "nested");
    assertEquals(mapping.get(DocPropertyType.OBJECT), "object");
    assertEquals(mapping.get(DocPropertyType.ROOT), "root");

  }
}