package com.avaje.ebeanservice.elastic;

import com.avaje.ebean.config.JsonConfig;
import com.avaje.ebean.plugin.BeanType;
import com.avaje.ebean.text.json.EJson;
import com.avaje.ebeanservice.docstore.api.DocStoreQueryUpdate;
import com.avaje.ebeanservice.docstore.api.DocStoreUpdate;
import com.avaje.ebeanservice.docstore.api.DocStoreUpdateProcessor;
import com.avaje.ebeanservice.docstore.api.DocStoreUpdates;
import com.avaje.ebeanservice.elastic.support.ElasticBatchUpdate;
import com.avaje.ebeanservice.elastic.support.IndexMessageSender;
import com.avaje.ebeanservice.elastic.support.IndexQueueWriter;
import com.avaje.ebeanservice.elastic.support.StringBuilderWriter;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * ElasticSearch implementation of the DocStoreUpdateProcessor.
 */
public class ElasticUpdateProcessor implements DocStoreUpdateProcessor {

  public static final Logger elaLogger = LoggerFactory.getLogger("org.avaje.ebean.ELA");

  protected final Logger logger = LoggerFactory.getLogger(ElasticUpdateProcessor.class);

  protected final JsonFactory jsonFactory;

  protected final IndexQueueWriter queueWriter;

  protected final IndexMessageSender messageSender;

  protected final int defaultBatchSize;

  protected final Object defaultObjectMapper;

  protected final JsonConfig.Include defaultInclude;

  public ElasticUpdateProcessor(IndexQueueWriter queueWriter, JsonFactory jsonFactory, Object defaultObjectMapper, IndexMessageSender messageSender, int defaultBatchSize) {
    this.queueWriter = queueWriter;
    this.jsonFactory = jsonFactory;
    this.defaultInclude = JsonConfig.Include.NON_EMPTY;
    this.defaultObjectMapper = defaultObjectMapper;
    this.messageSender = messageSender;
    this.defaultBatchSize = defaultBatchSize;
  }

  public ElasticBatchUpdate createBatchUpdate(int batchSize) throws IOException {

    int batch = (batchSize > 0) ? batchSize : defaultBatchSize;
    return new ElasticBatchUpdate(this, batch);
  }

  @Override
  public <T> DocStoreQueryUpdate<T> createQueryUpdate(BeanType<T> beanType, int batchSize) throws IOException {

    int batch = (batchSize > 0) ? batchSize : defaultBatchSize;
    return new ElasticQueryUpdate<T>(this, batch, beanType);
  }

  @Override
  public void process(DocStoreUpdates docStoreUpdates, int txnBulkBatchSize) {

    int batchSize = (txnBulkBatchSize > 0) ? txnBulkBatchSize : defaultBatchSize;
    sendBulkUpdate(docStoreUpdates, true, batchSize);
    sendQueueEvents(docStoreUpdates);
  }

  /**
   * Add the queue entries to the queue for later processing.
   */
  protected void sendQueueEvents(DocStoreUpdates docStoreUpdates) {

    queueWriter.queue(docStoreUpdates.getQueueEntries());
  }

  public Map<String, Object> sendPayload(ElasticBulkUpdate bulk) throws IOException {

    bulk.flush();

    String payload = bulk.getBuffer();
    if (elaLogger.isTraceEnabled()) {
      elaLogger.trace("ElasticBulkMessage Request:\n" + payload + "\n");
    }

    // send to Bulk API
    String response = messageSender.postBulk(payload);
    elaLogger.debug("request entries:{} responseSize:{}", payload.length(), response.length());

    if (elaLogger.isTraceEnabled()) {
      elaLogger.trace("ElasticBulkMessage Response:\n" + response);
    }

    // parse the response
    return parseBulkResponse(response);
  }


  public Map<String, Object> sendBulk(ElasticBulkUpdate bulk) throws IOException {

    return sendPayload(bulk);
  }

  /**
   * Send the 'bulk entries' to the ElasticSearch Bulk API.
   *
   * @param docStoreUpdates        The index updates holding the bulk entries to send
   * @param addToQueueOnFailure if true then failures are added tho the queue
   * @param batchSize           The batch size to use for sending to the Bulk API.
   */
  protected void sendBulkUpdate(DocStoreUpdates docStoreUpdates, boolean addToQueueOnFailure, int batchSize) {

    sendBulkUpdate(docStoreUpdates, batchSize, docStoreUpdates.getPersistEvents(), addToQueueOnFailure);
    sendBulkUpdate(docStoreUpdates, batchSize, docStoreUpdates.getDeleteEvents(), addToQueueOnFailure);
  }

  protected void sendBulkUpdate(DocStoreUpdates docStoreUpdates, int batchSize, List<? extends DocStoreUpdate> entries, boolean addToQueueOnFailure) {

    if (!entries.isEmpty()) {
      if (entries.size() <= batchSize) {
        // send them all in one go
        sendBulkUpdateBatch(docStoreUpdates, entries, addToQueueOnFailure);

      } else {
        // break into batches using the batchSize
        List<? extends List<? extends DocStoreUpdate>> batches = createBatches(entries, batchSize);
        for (int i = 0; i < batches.size(); i++) {
          sendBulkUpdateBatch(docStoreUpdates, batches.get(i), addToQueueOnFailure);
        }
      }
    }
  }

  /**
   * Send the bulk entries to ElasticSearch using the Bulk API.  If addToQueueOnFailure is set to true then
   * any entries that failed will be added to the queue.
   *
   * @param docStoreUpdates        The index updates holding the bulk entries to send
   * @param bulkEntries         The entries to send
   * @param addToQueueOnFailure if true then failures are added tho the queue
   */
  protected void sendBulkUpdateBatch(DocStoreUpdates docStoreUpdates, List<? extends DocStoreUpdate> bulkEntries, boolean addToQueueOnFailure) {

    try {
      ElasticBulkUpdate bulk = createBulkElasticUpdate();

      for (DocStoreUpdate entry : bulkEntries) {
        entry.docStoreUpdate(bulk);
      }

      bulk.flush();

      Map<String, Object> responseMap = sendPayload(bulk);

      Boolean errors = (Boolean) responseMap.get("errors");

      if (addToQueueOnFailure && errors) {
        // for any errors add the matching bulk request to the queue
        logger.debug("processing errors on response ...");
        List<Map<String, Object>> responseItems = (List<Map<String, Object>>) responseMap.get("responseItems");
        for (int i = 0; i < responseItems.size(); i++) {
          if (addToQueueForStatus(responseItems.get(i))) {
            logger.debug("... responseEntry:{} adding to queueWriter", i);
            bulkEntries.get(i).addToQueue(docStoreUpdates);
          }
        }
      }

    } catch (IOException e) {
      logger.error("Failed to successfully send bulk update to ElasticSearch", e);
      if (addToQueueOnFailure) {
        // add all remaining requests the the queue
        for (DocStoreUpdate entry : bulkEntries) {
          entry.addToQueue(docStoreUpdates);
        }
      }
    }

  }

  /**
   * Return true if this responseEntry should be added to the queue (typically based on it's returned status).
   */
  protected boolean addToQueueForStatus(Map<String, Object> responseEntry) {

    String status = (String) responseEntry.get("status");
    return "400".equals(status);
  }

  /**
   * Break up the requests into batches using the batchSize.
   */
  protected <T> List<List<T>> createBatches(List<T> allRequests, int batchSize) {

    List<List<T>> parts = new ArrayList<List<T>>();
    int totalSize = allRequests.size();

    for (int i = 0; i < totalSize; i += batchSize) {
      parts.add(allRequests.subList(i, Math.min(totalSize, i + batchSize)));
    }
    return parts;
  }

  /**
   * Parse the returned JSON response into a Map.
   */
  protected Map<String, Object> parseBulkResponse(String response) throws IOException {

    return EJson.parseObject(response);
  }

  /**
   * Create a BulkElasticUpdate.
   */
  public ElasticBulkUpdate createBulkElasticUpdate() throws IOException {

    StringBuilderWriter writer = new StringBuilderWriter();
    JsonGenerator gen = jsonFactory.createGenerator(writer);
    return new ElasticBulkUpdate(gen, writer, defaultObjectMapper, defaultInclude);
  }

}
