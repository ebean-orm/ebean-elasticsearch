package com.avaje.ebeanservice.elastic;

import com.avaje.ebean.plugin.BeanDocType;
import com.avaje.ebean.plugin.BeanType;
import com.avaje.ebeanservice.docstore.api.DocStoreQueryUpdate;
import com.avaje.ebeanservice.elastic.bulk.BulkBuffer;
import com.avaje.ebeanservice.elastic.bulk.BulkSender;

import java.io.IOException;
import java.util.Map;

/**
 * ElasticSearch implementation of DocStoreQueryUpdate.
 * <p>
 * Uses the ElasticSearch BULK API.
 * </p>
 */
public class ElasticQueryUpdate<T> implements DocStoreQueryUpdate<T> {

  private final BulkSender bulkSender;

  private final int batchSize;

  private final BeanDocType<T> beanDocType;

  private int count;

  private BulkBuffer current;

  public ElasticQueryUpdate(BulkSender bulkSender, int batchSize, BeanType<T> beanType) throws IOException {
    this.bulkSender = bulkSender;
    this.batchSize = batchSize;
    this.beanDocType = beanType.docStore();
    current = bulkSender.newBuffer();
  }

  @Override
  public void store(Object idValue, T bean) throws IOException {
    BulkBuffer obtain = obtain();
    beanDocType.index(idValue, bean, obtain);
  }

  /**
   * Flush the current buffer sending the Bulk API request to ElasticSearch.
   */
  @Override
  public void flush() throws IOException {

    // send the current buffer and collect any errors
    Map<String, Object> response = bulkSender.sendBulk(current);
    collectErrors(response);

    // create a new buffer and reset count to 0
    current = bulkSender.newBuffer();
    count = 0;
  }

  /**
   * Obtain a BulkElasticUpdate for writing bulk requests to.
   */
  private BulkBuffer obtain() throws IOException {
    if (count++ > batchSize) {
      flush();
    }
    return current;
  }

  /**
   * Collect all the error responses for reporting back on completion.
   */
  protected void collectErrors(Map<String, Object> response) {

  }

}
