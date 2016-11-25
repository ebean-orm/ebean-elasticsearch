package com.avaje.ebeanservice.elastic.update;

import com.avaje.ebean.PersistenceIOException;
import com.avaje.ebean.Query;
import com.avaje.ebean.plugin.BeanType;
import com.avaje.ebean.plugin.SpiServer;
import com.avaje.ebeanservice.docstore.api.support.DocStoreDeleteEvent;
import com.avaje.ebeanservice.docstore.api.support.DocStoreIndexEvent;
import com.avaje.ebeanservice.elastic.bulk.BulkUpdate;

import java.io.IOException;
import java.util.Collection;
import java.util.List;

/**
 */
public class ProcessGroup<T> {

  private final SpiServer server;

  private final BeanType<T> desc;

  private final UpdateGroup group;

  private final BulkUpdate txn;

  private long count;

  public static <T> long process(SpiServer server, BeanType<T> desc, UpdateGroup group, BulkUpdate txn) throws IOException {
    return new ProcessGroup<>(server, desc, group, txn).processGroup();
  }

  private ProcessGroup(SpiServer server, BeanType<T> desc, UpdateGroup group, BulkUpdate txn) {
    this.server = server;
    this.desc = desc;
    this.group = group;
    this.txn = txn;
  }

  private long processGroup() throws IOException {

    List<Object> deleteIds = group.getDeleteIds();
    for (Object id : deleteIds) {
      txn.send(new DocStoreDeleteEvent(desc, id));
    }

    count += deleteIds.size();

    List<Object> indexIds = group.getIndexIds();
    if (!indexIds.isEmpty()) {
      Query<T> query = server.find(desc.getBeanType());
      query.where().idIn(indexIds);
      indexUsingQuery(query, txn);
    }

    Collection<UpdateNested> values = group.getNestedPathIds().values();
    for (UpdateNested nested : values) {
      ProcessNested<T> nestedDocUpdate = new ProcessNested<>(server, desc, txn, nested);
      count += nestedDocUpdate.process();
    }

    return count;
  }


  private void indexUsingQuery(Query<T> query, final BulkUpdate txn) throws IOException {

    desc.docStore().applyPath(query);
    query.setLazyLoadBatchSize(100);
    query.findEach(bean -> {
      Object idValue = desc.getBeanId(bean);
      try {
        count++;
        txn.send(new DocStoreIndexEvent<>(desc, idValue, bean));
      } catch (Exception e) {
        throw new PersistenceIOException("Error performing query update to doc store", e);
      }
    });
  }
}
