package io.ebeanservice.elastic.bulk;

import io.ebeanservice.docstore.api.DocStoreUpdate;

import javax.persistence.PersistenceException;
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
  }

  /**
   * Send the event via Bulk API.
   */
  public void send(DocStoreUpdate event) throws IOException {
    event.docStoreUpdate(obtain());
  }

  /**
   * Obtain a BulkBuffer for writing bulk requests to.
   * <p>
   * This automatically manages the bulk buffer batch size and flushing.
   * </p>
   */
  public BulkBuffer obtain() {
    try {
      if (currentBuffer == null) {
        return newBuffer();
      }
      if (++count > batchSize) {
        flush();
        return newBuffer();
      }
      return currentBuffer;
    } catch (IOException e) {
      throw new PersistenceException("Error obtaining a buffer for Bulk updates", e);
    }
  }

  /**
   * Flush the current buffer sending the Bulk API request to ElasticSearch.
   */
  public void flush() {

    try {
      if (currentBuffer != null) {
        collectErrors(bulkSender.sendBulk(currentBuffer));
      }
    } catch (IOException e) {
      throw new PersistenceException("Error send Bulk updates", e);
    }
  }

  private BulkBuffer newBuffer() throws IOException {
    count = 1;
    currentBuffer = bulkSender.newBuffer();
    return currentBuffer;
  }

  /**
   * Collect all the error responses for reporting back on completion.
   */
  protected void collectErrors(Map<String, Object> response) {

  }

  public void sendUpdateQuery(String indexName, String indexType, String script) throws IOException {
    bulkSender.setUpdateQuery(indexName, indexType, script);
  }
}
