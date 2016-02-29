package com.avaje.ebeanservice.elastic.updategroup;

import com.avaje.ebean.PersistenceIOException;
import com.avaje.ebean.Query;
import com.avaje.ebean.QueryEachConsumer;
import com.avaje.ebean.plugin.BeanType;
import com.avaje.ebean.plugin.SpiServer;
import com.avaje.ebeanservice.docstore.api.support.DocStoreDeleteEvent;
import com.avaje.ebeanservice.docstore.api.support.DocStoreIndexEvent;
import com.avaje.ebeanservice.elastic.support.ElasticBatchUpdate;

import java.io.IOException;
import java.util.Collection;
import java.util.List;

/**
 */
public class ProcessGroup<T> {

  final SpiServer server;

  final BeanType<T> desc;

  final UpdateGroup group;

  final ElasticBatchUpdate txn;

  public static <T> void process(SpiServer server, BeanType<T> desc, UpdateGroup group, ElasticBatchUpdate txn) throws IOException {
    new ProcessGroup<T>(server, desc, group, txn).processGroup();
  }

  private ProcessGroup(SpiServer server, BeanType<T> desc, UpdateGroup group, ElasticBatchUpdate txn) {
    this.server = server;
    this.desc = desc;
    this.group = group;
    this.txn = txn;
  }

  private void processGroup() throws IOException {

    List<Object> deleteIds = group.getDeleteIds();
    for (Object id : deleteIds) {
      txn.addEvent(new DocStoreDeleteEvent(desc, id));
    }

    List<Object> indexIds = group.getIndexIds();
    if (!indexIds.isEmpty()) {
      Query<T> query = server.find(desc.getBeanType());
      query.where().idIn(indexIds);
      indexUsingQuery(query, txn);
    }

    Collection<UpdateNested> values = group.getNestedPathIds().values();
    for (UpdateNested nested : values) {
      ProcessNested<T> nestedDocUpdate = new ProcessNested<T>(server, desc, txn, nested);
      nestedDocUpdate.process();
    }
  }


  private void indexUsingQuery(Query<T> query, final ElasticBatchUpdate txn) throws IOException {

    desc.docStore().applyPath(query);
    query.setLazyLoadBatchSize(100);
    query.findEach(new QueryEachConsumer<T>() {
      @Override
      public void accept(T bean) {
        Object idValue = desc.getBeanId(bean);
        try {
          txn.addEvent(new DocStoreIndexEvent<T>(desc, idValue, bean));
        } catch (Exception e) {
          throw new PersistenceIOException("Error performing query update to doc store", e);
        }
      }
    });
  }
}
