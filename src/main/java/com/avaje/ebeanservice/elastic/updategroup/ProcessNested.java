package com.avaje.ebeanservice.elastic.updategroup;

import com.avaje.ebean.EbeanServer;
import com.avaje.ebean.Query;
import com.avaje.ebean.QueryEachConsumer;
import com.avaje.ebean.plugin.SpiBeanType;
import com.avaje.ebean.plugin.SpiProperty;
import com.avaje.ebean.text.PathProperties;
import com.avaje.ebeanservice.elastic.support.ElasticBatchUpdate;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 *
 */
public class ProcessNested<T> {

  final EbeanServer server;
  final SpiBeanType<T> desc;
  final ElasticBatchUpdate txn;
  final UpdateNested nested;

  final Map<Object, String> jsonMap = new HashMap<Object,String>();

  final String path;
  final SpiBeanType<?> targetDesc;

  final PathProperties nestedDoc;

  public ProcessNested(EbeanServer server, SpiBeanType<T> desc, ElasticBatchUpdate txn, UpdateNested nested) {
    this.server = server;
    this.desc = desc;
    this.txn = txn;
    this.nested = nested;
    this.path = nested.getPath();
    this.nestedDoc = desc.docStoreNested(path);
    this.targetDesc = desc.getBeanTypeAtPath(path);
  }

  public void process() throws IOException {

    List<Object> nestedIds = nested.getIds();

    fetchEmbedded(nestedIds);
    processTop(nestedIds);
  }

  /**
   * Populate a map of all the embedded JSON documents that we then want to send as updates to the parent.
   */
  protected void fetchEmbedded(List<Object> nestedIds) throws IOException {

    Query<?> pathQuery = server.createQuery(targetDesc.getBeanType());
    pathQuery.apply(nestedDoc);
    pathQuery.where().idIn(nestedIds);

    // hit the database and build the embedded JSON documents
    List<?> list = pathQuery.findList();
    for (Object bean : list) {
      String embedJson = server.json().toJson(bean, nestedDoc);
      Object beanId = targetDesc.beanId(bean);
      jsonMap.put(beanId, embedJson);
    }
  }

  protected void processTop(List<Object> nestedIds) {

    Query<T> topQuery = server.createQuery(desc.getBeanType());
    topQuery.select("id,"+nested.getPath());
    topQuery.where().in(nested.getPath()+".id", nestedIds);

    final SpiProperty property = desc.property(nested.getPath());

    topQuery.findEach(new QueryEachConsumer<T>() {
      @Override
      public void accept(T bean)  {
        try {
          Object beanId = desc.getBeanId(bean);
          Object embBean = property.getVal(bean);
          Object targetId = targetDesc.beanId(embBean);

          String json = jsonMap.get(targetId);

          desc.docStoreUpdateEmbedded(beanId, path, json, txn.obtain());

          System.out.println("send it beanId:" + beanId + " json:" + json);
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      }
    });
  }


}
