package com.avaje.ebeanservice.elastic.bulk;

import com.avaje.ebeanservice.docstore.api.DocStoreUpdate;
import com.avaje.ebeanservice.elastic.ElasticUpdateProcessor;

import java.io.IOException;
import java.util.Map;

/**
 * Batches calls to the BULK API based on batch size.
 */
public class BulkUpdate {

  private final BulkSender bulkSender;

  private final int batchSize;

  private int count;

  private BulkBuffer currentBuffer;

  public BulkUpdate(int batchSize, BulkSender bulkSender) throws IOException {
    this.bulkSender = bulkSender;
    this.batchSize = batchSize;
    currentBuffer = bulkSender.newBuffer();
  }

  /**
   * Send the event via Bulk API.
   */
  public void send(DocStoreUpdate event) throws IOException {
    event.docStoreUpdate(obtain());
  }

  /**
   * Flush the current buffer sending the Bulk API request to ElasticSearch.
   */
  public void flush() throws IOException {

    // send the current buffer and collect any errors
    Map<String, Object> response = bulkSender.sendBulk(currentBuffer);
    collectErrors(response);

    // create a new buffer and reset count to 0
    currentBuffer = bulkSender.newBuffer();
    count = 0;
  }

  /**
   * Obtain a BulkBuffer for writing bulk requests to.
   * <p>
   * This automatically manages the bulk buffer batch size and flushing.
   * </p>
   */
  public BulkBuffer obtain() throws IOException {
    if (count++ > batchSize) {
      flush();
    }
    return currentBuffer;
  }

  /**
   * Collect all the error responses for reporting back on completion.
   */
  protected void collectErrors(Map<String, Object> response) {

  }

}
