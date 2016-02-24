package com.avaje.ebeanservice.elastic;

import com.avaje.ebean.PersistenceIOException;
import com.avaje.ebean.QueryEachConsumer;
import com.avaje.ebean.text.json.EJson;
import com.avaje.ebeanservice.elastic.search.RawSource;
import com.avaje.ebeanservice.elastic.support.ElasticBatchUpdate;
import com.fasterxml.jackson.core.JsonGenerator;

import java.io.IOException;

/**
 */
public class CopyRaw implements QueryEachConsumer<RawSource> {

  final ElasticBatchUpdate txn;

  final String indexType;

  final String indexName;

  public CopyRaw(ElasticBatchUpdate txn, String indexType, String indexName) {
    this.txn = txn;
    this.indexType = indexType;
    this.indexName = indexName;
  }

  @Override
  public void accept(RawSource bean) {

    try {
      JsonGenerator gen = txn.obtain().gen();
      writeBulkHeader(gen, bean.getId());
      EJson.write(bean.getSource(), gen);
      gen.writeRaw("\n");

    } catch (IOException e) {
      throw new PersistenceIOException(e);
    }
  }

  private void writeBulkHeader(JsonGenerator gen, Object idValue) throws IOException {

    gen.writeStartObject();
    gen.writeFieldName("index");
    gen.writeStartObject();
    gen.writeStringField("_id", idValue.toString());
    gen.writeStringField("_type", indexType);
    gen.writeStringField("_index", indexName);
    gen.writeEndObject();
    gen.writeEndObject();
    gen.writeRaw("\n");
  }
}
