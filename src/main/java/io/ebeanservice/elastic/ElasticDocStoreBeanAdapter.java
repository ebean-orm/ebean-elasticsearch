package io.ebeanservice.elastic;

import io.ebean.bean.EntityBean;
import io.ebean.docstore.DocUpdateContext;
import io.ebeaninternal.server.core.PersistRequestBean;
import io.ebeaninternal.server.deploy.BeanDescriptor;
import io.ebeaninternal.server.deploy.meta.DeployBeanDescriptor;
import io.ebeaninternal.server.text.json.WriteJson;
import io.ebeanservice.docstore.api.DocStoreUpdateContext;
import io.ebeanservice.docstore.api.support.DocStoreBeanBaseAdapter;
import io.ebeanservice.elastic.bulk.BulkBuffer;
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
  public void deleteById(Object idValue, DocUpdateContext docTxn) throws IOException {
    BulkBuffer txn = asElasticBulkUpdate(docTxn);
    JsonGenerator gen = txn.gen();
    writeBulkHeader(gen, idValue, "delete");
  }

  @Override
  public void insert(Object idValue, PersistRequestBean<T> persistRequest, DocStoreUpdateContext txn) throws IOException {
    index(idValue, persistRequest.getBean(), txn);
  }

  @Override
  public void index(Object idValue, T entityBean, DocUpdateContext docTxn) throws IOException {
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

  private BulkBuffer asElasticBulkUpdate(DocUpdateContext docTxn) {
    return (BulkBuffer)docTxn;
  }

  @Override
  public void updateEmbedded(Object idValue, String embeddedProperty, String embeddedRawContent, DocUpdateContext docTxn) throws IOException {
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
    gen.writeStringField("_index", indexName);
    gen.writeEndObject();
    gen.writeEndObject();
    gen.writeRaw("\n");
  }

}
