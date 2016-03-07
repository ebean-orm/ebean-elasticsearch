package com.avaje.ebeanservice.elastic.search.rawsource;

import com.avaje.ebean.PersistenceIOException;
import com.avaje.ebean.QueryEachConsumer;
import com.avaje.ebean.text.json.EJson;
import com.avaje.ebeanservice.elastic.bulk.BulkUpdate;
import com.fasterxml.jackson.core.JsonGenerator;

import java.io.IOException;

/**
 * Used to scroll a source index and copy to another index.
 */
public class RawSourceCopier implements QueryEachConsumer<RawSource> {

  private final BulkUpdate txn;

  private final String targetIndexType;

  private final String targetIndexName;

  /**
   * Construct with target index type and name.
   */
  public RawSourceCopier(BulkUpdate txn, String targetIndexType, String targetIndexName) {
    this.txn = txn;
    this.targetIndexType = targetIndexType;
    this.targetIndexName = targetIndexName;
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
    gen.writeStringField("_type", targetIndexType);
    gen.writeStringField("_index", targetIndexName);
    gen.writeEndObject();
    gen.writeEndObject();
    gen.writeRaw("\n");
  }
}
