package com.avaje.ebeanservice.elastic.index;

import com.fasterxml.jackson.core.JsonGenerator;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Holds index alias Add and Remove changes.
 */
public class AliasChanges {

  private final List<Entry> entries = new ArrayList<Entry>();

  /**
   * Add an index alias ADD change.
   *
   * @param index The index name
   * @param alias The alias name
   */
  public AliasChanges add(String index, String alias) {
    entries.add(new Entry(true, index, alias));
    return this;
  }

  /**
   * Add an index alias REMOVE change.
   *
   * @param index The index name
   * @param alias The alias name
   */
  public AliasChanges remove(String index, String alias) {
    entries.add(new Entry(false, index, alias));
    return this;
  }

  /**
   * Return true if there are no changes.
   */
  public boolean isEmpty() {
    return entries.isEmpty();
  }

  public void writeJson(JsonGenerator gen) throws IOException {
    gen.writeStartObject();
    gen.writeFieldName("actions");
    gen.writeStartArray();
    for (Entry entry : entries) {
      entry.writeJson(gen);
    }
    gen.writeEndArray();
    gen.writeEndObject();
  }

  private class Entry {

    private final boolean add;
    private final String index;
    private final String alias;

    Entry(boolean add, String index, String alias) {
      this.add = add;
      this.index = index;
      this.alias = alias;
    }

    void writeJson(JsonGenerator gen) throws IOException {

      gen.writeStartObject();
      gen.writeFieldName(add? "add" : "remove");
      gen.writeStartObject();
      gen.writeStringField("index", index);
      gen.writeStringField("alias", alias);
      gen.writeEndObject();
      gen.writeEndObject();
    }
  }
}
