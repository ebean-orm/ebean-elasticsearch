package io.ebeanservice.elastic.index;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.PrettyPrinter;
import com.fasterxml.jackson.core.util.DefaultPrettyPrinter;
import io.ebean.PersistenceIOException;
import io.ebean.core.type.DocPropertyType;
import io.ebean.plugin.BeanType;
import io.ebeanservice.docstore.api.mapping.DocPropertyAdapter;
import io.ebeanservice.docstore.api.mapping.DocPropertyMapping;
import io.ebeanservice.docstore.api.mapping.DocPropertyOptions;
import io.ebeanservice.docstore.api.mapping.DocumentMapping;
import io.ebeanservice.elastic.support.StringBuilderWriter;

import java.io.IOException;

/**
 * Builds mapping JSON for an Index using the DocumentMapping.
 */
public class EIndexMappingsBuilder {

  private final JsonFactory jsonFactory;

  private final EIndexTypeMapping typeMapping;

  private final PrettyPrinter compactJson = new CompactJsonPrettyPrinter();

  private final PrettyPrinter prettyJson = new DefaultPrettyPrinter();

  public EIndexMappingsBuilder(JsonFactory jsonFactory) {
    this.jsonFactory = jsonFactory;
    this.typeMapping = new EIndexTypeMapping();
  }

  /**
   * Return the mapping json for a given bean type.
   */
  public String createMappingJson(BeanType<?> beanType) {

    try {
      DocumentMapping docMapping = (DocumentMapping)beanType.getDocMapping();

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
      gen.writeObjectFieldStart("properties");

      IndexVisitor visitor = new IndexVisitor(gen, typeMapping);

      docMapping.visit(visitor);

      gen.writeEndObject();
      gen.writeEndObject();
      gen.writeEndObject();
      gen.flush();

      return writer.toString();

    } catch (IOException e) {
      throw new PersistenceIOException(e);
    }
  }

  private class IndexVisitor extends DocPropertyAdapter {

    private final JsonGenerator gen;

    private final EIndexTypeMapping typeMapping;

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

        DocPropertyOptions options = property.getOptions();
        if (options != null && isFalse(options.getEnabled())) {
          gen.writeBooleanField("enabled", false);
        } else {

          // map from general document type to elastic type
          DocPropertyType logicalType = property.getType();
          gen.writeStringField("type", typeMapping.get(logicalType));

          if (options != null) {
            if (options.isOptionsSet()) {
              gen.writeStringField("index_options", options.getOptions().name().toLowerCase());
            }
            if (isFalse(options.getNorms())) {
              gen.writeBooleanField("norms", false);
            }
            if (isFalse(options.getDocValues())) {
              gen.writeBooleanField("docValues", false);
            }
            if (isTrue(options.getStore())) {
              gen.writeBooleanField("store", true);
            }
            if (options.getBoost() != null) {
              gen.writeNumberField("boost", options.getBoost());
            }
            if (options.getNullValue() != null) {
              gen.writeStringField("null_value", options.getNullValue());
            }
            if (options.getCopyTo() != null) {
              gen.writeStringField("copy_to", options.getCopyTo());
            }
            if (options.getAnalyzer() != null) {
              gen.writeStringField("analyzer", options.getAnalyzer());
            }
            if (options.getSearchAnalyzer() != null) {
              gen.writeStringField("search_analyzer", options.getSearchAnalyzer());
            }
            if (isTrue(options.getSortable())) {
              // add raw field option
              gen.writeObjectFieldStart("fields");
              gen.writeObjectFieldStart("raw");
              gen.writeStringField("type", "keyword");
              gen.writeEndObject();
              gen.writeEndObject();
            }
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

    private boolean isFalse(Boolean option) {
      return Boolean.FALSE.equals(option);
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

  /**
   * Return true if this type should not be analysed.
   */
  private boolean notAnalysed(DocPropertyType logicalType) {
    switch (logicalType) {
      case UUID: return true;
      case ENUM: return true;
      case KEYWORD: return true;
      default:
        return false;
    }
  }
}
