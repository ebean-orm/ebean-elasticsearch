package com.avaje.ebeanservice.elastic;

import com.avaje.ebean.plugin.BeanDocType;
import com.avaje.ebean.plugin.BeanType;
import com.avaje.ebeanservice.docstore.api.DocStoreQueryUpdate;
import com.avaje.ebeanservice.elastic.bulk.BulkUpdate;

import java.io.IOException;

/**
 * ElasticSearch implementation of DocStoreQueryUpdate.
 * <p>
 * Uses the ElasticSearch BULK API.
 * </p>
 */
public class ElasticQueryUpdate<T> implements DocStoreQueryUpdate<T> {

  private final BeanDocType<T> beanDocType;

  private final BulkUpdate bulkUpdate;

  public ElasticQueryUpdate(BulkUpdate bulkUpdate, BeanType<T> beanType) throws IOException {
    this.bulkUpdate = bulkUpdate;
    this.beanDocType = beanType.docStore();
  }

  @Override
  public void store(Object idValue, T bean) throws IOException {
    beanDocType.index(idValue, bean, bulkUpdate.obtain());
  }

  /**
   * Flush the current buffer sending the Bulk API request to ElasticSearch.
   */
  @Override
  public void flush() throws IOException {
    bulkUpdate.flush();
  }

}
