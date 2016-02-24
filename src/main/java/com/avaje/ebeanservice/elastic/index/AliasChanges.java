package com.avaje.ebeanservice.elastic.index;

import com.fasterxml.jackson.core.JsonGenerator;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by rob on 24/02/16.
 */
public class AliasChanges {

  List<Entry> entries = new ArrayList<Entry>();

  public AliasChanges add(String index, String alias) {
    entries.add(new Entry(true, index, alias));
    return this;
  }

  public AliasChanges remove(String index, String alias) {
    entries.add(new Entry(false, index, alias));
    return this;
  }

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

  public class Entry {

    final boolean add;
    final String index;
    final String alias;

    public Entry(boolean add, String index, String alias) {
      this.add = add;
      this.index = index;
      this.alias = alias;
    }

    public void writeJson(JsonGenerator gen) throws IOException {

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
