package com.avaje.ebeanservice.elastic;

import com.avaje.ebean.bean.EntityBean;
import com.avaje.ebeaninternal.server.core.PersistRequestBean;
import com.avaje.ebeaninternal.server.deploy.BeanDescriptor;
import com.avaje.ebeaninternal.server.deploy.meta.DeployBeanDescriptor;
import com.avaje.ebeaninternal.server.text.json.WriteJson;
import com.avaje.ebeanservice.docstore.api.DocStoreUpdateContext;
import com.avaje.ebeanservice.docstore.api.support.DocStoreBeanBaseAdapter;
import com.avaje.ebeanservice.elastic.bulk.BulkBuffer;
import com.fasterxml.jackson.core.JsonGenerator;

import java.io.IOException;

/**
 * Helper for BeanDescriptor to handle the ElasticSearch features.
 */
public class ElasticDocStoreBeanAdapter<T> extends DocStoreBeanBaseAdapter<T> {

  public ElasticDocStoreBeanAdapter(BeanDescriptor<T> desc, DeployBeanDescriptor<T> deploy) {
    super(desc, deploy);
  }

  @Override
  public void deleteById(Object idValue, DocStoreUpdateContext docTxn) throws IOException {

    BulkBuffer txn = asElasticBulkUpdate(docTxn);
    JsonGenerator gen = txn.gen();
    writeBulkHeader(gen, idValue, "delete");
  }

  @Override
  public void insert(Object idValue, PersistRequestBean<T> persistRequest, DocStoreUpdateContext txn) throws IOException {

    index(idValue, persistRequest.getBean(), txn);
  }

  @Override
  public void index(Object idValue, T entityBean, DocStoreUpdateContext docTxn) throws IOException {

    BulkBuffer txn = asElasticBulkUpdate(docTxn);

    JsonGenerator gen = txn.gen();
    writeBulkHeader(gen, idValue, "index");

    // use the pathProperties for 'index' requests
    WriteJson writeJson = txn.createWriteJson(server, gen, docStructure.doc());
    desc.jsonWrite(writeJson, (EntityBean) entityBean);
    gen.writeRaw("\n");
  }

  @Override
  public void update(Object idValue, PersistRequestBean<T> persistRequest, DocStoreUpdateContext docTxn) throws IOException {

    BulkBuffer txn = asElasticBulkUpdate(docTxn);
    JsonGenerator gen = txn.gen();
    writeBulkHeader(gen, idValue, "update");

    gen.writeStartObject();
    gen.writeFieldName("doc");
    // only the 'dirty' properties included in 'update' request
    WriteJson writeJson = txn.createWriteJson(server, gen, null);
    desc.jsonWriteDirty(writeJson, persistRequest.getEntityBean(), persistRequest.getDirtyProperties());
    gen.writeEndObject();
    gen.writeRaw("\n");
  }

  private BulkBuffer asElasticBulkUpdate(DocStoreUpdateContext docTxn) {
    return (BulkBuffer)docTxn;
  }

  @Override
  public void updateEmbedded(Object idValue, String embeddedProperty, String embeddedRawContent, DocStoreUpdateContext docTxn) throws IOException {

    BulkBuffer txn = asElasticBulkUpdate(docTxn);

    JsonGenerator gen = txn.gen();
    writeBulkHeader(gen, idValue, "update");

    gen.writeStartObject();
    gen.writeFieldName("doc");
    gen.writeStartObject();
    gen.writeFieldName(embeddedProperty);
    gen.writeRaw(":");
    gen.writeRaw(embeddedRawContent);
    gen.writeEndObject();
    gen.writeEndObject();
    gen.writeRaw("\n");
  }

  private void writeBulkHeader(JsonGenerator gen, Object idValue, String event) throws IOException {

    gen.writeStartObject();
    gen.writeFieldName(event);
    gen.writeStartObject();
    if (idValue != null) {
      // use elasticsearch generated id value
      gen.writeStringField("_id", idValue.toString());
    }
    gen.writeStringField("_type", indexType);
    gen.writeStringField("_index", indexName);
    gen.writeEndObject();
    gen.writeEndObject();
    gen.writeRaw("\n");
  }

}
