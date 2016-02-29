package com.avaje.ebeanservice.elastic;

import com.avaje.ebean.plugin.BeanDocType;
import com.avaje.ebean.plugin.BeanType;
import com.avaje.ebeanservice.docstore.api.DocStoreQueryUpdate;

import java.io.IOException;
import java.util.Map;

/**
 * ElasticSearch implementation of DocStoreQueryUpdate.
 * <p>
 * Uses the ElasticSearch BULK API.
 * </p>
 */
public class ElasticQueryUpdate<T> implements DocStoreQueryUpdate<T> {

  private final ElasticUpdateProcessor indexUpdateProcessor;

  private final BeanType<T> beanType;

  private final int batchSize;

  private final BeanDocType<T> beanDocType;

  private int count;

  private ElasticBulkUpdate current;

  public ElasticQueryUpdate(ElasticUpdateProcessor indexUpdateProcessor, int batchSize, BeanType<T> beanType) throws IOException {
    this.indexUpdateProcessor = indexUpdateProcessor;
    this.batchSize = batchSize;
    this.beanType = beanType;
    this.beanDocType = beanType.docStore();
    current = indexUpdateProcessor.createBulkElasticUpdate();
  }

  @Override
  public void store(Object idValue, T bean) throws IOException {
    ElasticBulkUpdate obtain = obtain();
    beanDocType.index(idValue, bean, obtain);
  }

  /**
   * Flush the current buffer sending the Bulk API request to ElasticSearch.
   */
  @Override
  public void flush() throws IOException {

    // send the current buffer and collect any errors
    Map<String, Object> response = indexUpdateProcessor.sendBulk(current);
    collectErrors(response);

    // create a new buffer and reset count to 0
    current = indexUpdateProcessor.createBulkElasticUpdate();
    count = 0;
  }

  /**
   * Obtain a BulkElasticUpdate for writing bulk requests to.
   */
  private ElasticBulkUpdate obtain() throws IOException {
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
