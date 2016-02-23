package com.avaje.ebeanservice.elastic.index;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.util.MinimalPrettyPrinter;

import java.io.IOException;

/**
 * Compact JSON pretty printer.
 */
public class CompactJsonPrettyPrinter extends MinimalPrettyPrinter {

  @Override
  public void writeEndObject(JsonGenerator jg, int nrOfEntries) throws IOException {
    jg.writeRaw(" }");
  }

  @Override
  public void writeStartArray(JsonGenerator jg) throws IOException {
    jg.writeRaw(" [");
  }

  @Override
  public void writeArrayValueSeparator(JsonGenerator jg) throws IOException {
    jg.writeRaw(", ");
  }

  @Override
  public void writeEndArray(JsonGenerator jg, int nrOfValues) throws IOException {
    jg.writeRaw(" ]");
  }

  @Override
  public void beforeObjectEntries(JsonGenerator jg) throws IOException {
    jg.writeRaw(" ");
  }

  @Override
  public void writeObjectFieldValueSeparator(JsonGenerator jg) throws IOException {
    jg.writeRaw(": ");
  }

  @Override
  public void writeObjectEntrySeparator(JsonGenerator jg) throws IOException {
    jg.writeRaw(", ");
  }
}
