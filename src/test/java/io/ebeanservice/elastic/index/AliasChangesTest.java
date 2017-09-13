package io.ebeanservice.elastic.index;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import org.testng.annotations.Test;

import java.io.IOException;
import java.io.StringWriter;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.AssertJUnit.assertFalse;

public class AliasChangesTest {

  JsonFactory jsonFactory = new JsonFactory();

  @Test
  public void add() throws Exception {

    AliasChanges changes = new AliasChanges();
    changes.add("a","b");

    assertEquals(getJson(changes),"{\"actions\":[{\"add\":{\"index\":\"a\",\"alias\":\"b\"}}]}");
  }


  @Test
  public void remove() throws Exception {

    AliasChanges changes = new AliasChanges();
    changes.remove("a","b");

    assertEquals(getJson(changes),"{\"actions\":[{\"remove\":{\"index\":\"a\",\"alias\":\"b\"}}]}");
  }

  @Test
  public void addPlusRemove() throws Exception {

    AliasChanges changes = new AliasChanges();
    changes.add("a","b");
    changes.remove("c","d");

    assertEquals(getJson(changes),"{\"actions\":[{\"add\":{\"index\":\"a\",\"alias\":\"b\"}},{\"remove\":{\"index\":\"c\",\"alias\":\"d\"}}]}");
  }

  @Test
  public void isEmpty_when_empty() throws Exception {
    AliasChanges changes = new AliasChanges();
    assertTrue(changes.isEmpty());
  }

  @Test
  public void isEmpty_when_notEmpty() throws Exception {
    AliasChanges changes = new AliasChanges();
    changes.add("a","b");
    assertFalse(changes.isEmpty());
  }


  private String getJson(AliasChanges changes) throws IOException {

    StringWriter writer = new StringWriter();
    JsonGenerator generator = jsonFactory.createGenerator(writer);
    changes.writeJson(generator);
    generator.flush();
    generator.close();
    return writer.toString();
  }

}