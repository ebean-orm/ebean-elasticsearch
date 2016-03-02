package com.avaje.ebeanservice.elastic.index;

import com.avaje.ebean.PersistenceIOException;
import com.avaje.ebean.plugin.BeanType;
import com.avaje.ebeanservice.docstore.api.mapping.DocPropertyAdapter;
import com.avaje.ebeanservice.docstore.api.mapping.DocPropertyMapping;
import com.avaje.ebeanservice.docstore.api.mapping.DocPropertyOptions;
import com.avaje.ebeanservice.docstore.api.mapping.DocumentMapping;
import com.avaje.ebeanservice.elastic.support.StringBuilderWriter;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.PrettyPrinter;
import com.fasterxml.jackson.core.util.DefaultPrettyPrinter;

import java.io.IOException;

/**
 * Builds mapping JSON for an Index using the DocumentMapping.
 */
public class EIndexMappingsBuilder {

  final JsonFactory jsonFactory;

  final EIndexTypeMapping typeMapping;

  final PrettyPrinter compactJson = new CompactJsonPrettyPrinter();

  final PrettyPrinter prettyJson = new DefaultPrettyPrinter();

  public EIndexMappingsBuilder(JsonFactory jsonFactory) {
    this.jsonFactory = jsonFactory;
    this.typeMapping = new EIndexTypeMapping();
  }

  public String createMappingJson(BeanType<?> beanType) {

    try {
      DocumentMapping docMapping = beanType.getDocMapping();

      StringBuilderWriter writer = new StringBuilderWriter();
      JsonGenerator gen = jsonFactory.createGenerator(writer);
      gen.setPrettyPrinter(prettyJson);

      gen.writeStartObject();

      int shards = docMapping.getShards();
      int replicas = docMapping.getReplicas();

      if (shards > 0 || replicas > 0) {
        gen.writeObjectFieldStart("settings");
        if (shards > 0) {
          gen.writeNumberField("number_of_shards", shards);
        }
        if (replicas > 0) {
          gen.writeNumberField("number_of_replicas", replicas);
        }
        gen.writeEndObject();
      }

      gen.writeObjectFieldStart("mappings");
      gen.writeObjectFieldStart(docMapping.getType());
      gen.writeObjectFieldStart("properties");

      IndexVisitor visitor = new IndexVisitor(gen, typeMapping);

      docMapping.visit(visitor);

      gen.writeEndObject();
      gen.writeEndObject();
      gen.writeEndObject();
      gen.writeEndObject();
      gen.flush();


      return writer.toString();

    } catch (IOException e) {
      throw new PersistenceIOException(e);
    }
  }

  class IndexVisitor extends DocPropertyAdapter {

    final JsonGenerator gen;

    final EIndexTypeMapping typeMapping;

    public IndexVisitor(JsonGenerator gen, EIndexTypeMapping typeMapping) {
      this.gen = gen;
      this.typeMapping = typeMapping;
    }

    @Override
    public void visitProperty(DocPropertyMapping property) {

      try {
        gen.writeFieldName(property.getName());
        gen.setPrettyPrinter(compactJson);
        gen.writeStartObject();

        // map from general document type to elastic type
        String type = typeMapping.get(property.getType());

        gen.writeStringField("type", type);

        DocPropertyOptions options = property.getOptions();
        if (options != null) {
          if (isTrue(options.getStore())) {
            gen.writeBooleanField("store", true);
          }
          if (options.getBoost() != null) {
            gen.writeNumberField("boost", options.getBoost());
          }
          if (options.getNullValue() != null) {
            gen.writeStringField("null_value", options.getNullValue());
          }
          if (isTrue(options.getCode())) {
            gen.writeStringField("index", "not_analyzed");

          } else if (isTrue(options.getSortable())) {
            // add raw field option
            gen.writeObjectFieldStart("fields");
            gen.writeObjectFieldStart("raw");
            gen.writeStringField("type", "string");
            gen.writeStringField("index", "not_analyzed");
            gen.writeEndObject();
            gen.writeEndObject();
          }
        }
        gen.writeEndObject();
        gen.setPrettyPrinter(prettyJson);

      } catch (IOException e) {
        throw new PersistenceIOException(e);
      }
    }

    private boolean isTrue(Boolean option) {
      return Boolean.TRUE.equals(option);
    }

    @Override
    public void visitBeginObject(DocPropertyMapping property) {
      try {
        gen.writeObjectFieldStart(property.getName());
        gen.writeObjectFieldStart("properties");
      } catch (IOException e) {
        throw new PersistenceIOException(e);
      }
    }

    @Override
    public void visitEndObject(DocPropertyMapping property) {
      try {
        gen.writeEndObject();
        gen.writeEndObject();
      } catch (IOException e) {
        throw new PersistenceIOException(e);
      }
    }

    @Override
    public void visitBeginList(DocPropertyMapping property) {
      try {
        gen.writeObjectFieldStart(property.getName());
        gen.writeStringField("type", "nested");
        gen.writeObjectFieldStart("properties");
      } catch (IOException e) {
        throw new PersistenceIOException(e);
      }
    }

    @Override
    public void visitEndList(DocPropertyMapping property) {
      try {
        gen.writeEndObject();
        gen.writeEndObject();
      } catch (IOException e) {
        throw new PersistenceIOException(e);
      }
    }
  }
}
