package io.ebeanservice.elastic;

import com.fasterxml.jackson.core.JsonFactory;
import io.avaje.applog.AppLog;
import io.ebean.DocStoreQueueEntry;
import io.ebean.config.JsonConfig;
import io.ebean.plugin.BeanType;
import io.ebean.plugin.SpiServer;
import io.ebeanservice.docstore.api.*;
import io.ebeanservice.elastic.bulk.BulkSender;
import io.ebeanservice.elastic.bulk.BulkTransaction;
import io.ebeanservice.elastic.bulk.BulkUpdate;
import io.ebeanservice.elastic.support.IndexMessageSender;
import io.ebeanservice.elastic.support.IndexQueueWriter;
import io.ebeanservice.elastic.update.ConvertToGroups;
import io.ebeanservice.elastic.update.ProcessGroup;
import io.ebeanservice.elastic.update.UpdateGroup;

import javax.persistence.PersistenceException;
import java.io.IOException;
import java.util.Collection;
import java.util.List;

import static java.lang.System.Logger.Level.DEBUG;
import static java.lang.System.Logger.Level.ERROR;

/**
 * ElasticSearch implementation of the DocStoreUpdateProcessor.
 */
public class ElasticUpdateProcessor implements DocStoreUpdateProcessor {

  private final System.Logger logger = AppLog.getLogger(ElasticUpdateProcessor.class);

  private final SpiServer server;

  private final IndexQueueWriter queueWriter;

  private final int defaultBatchSize;

  private final BulkSender bulkSender;

  public ElasticUpdateProcessor(SpiServer server, IndexQueueWriter queueWriter, JsonFactory jsonFactory,
                                Object defaultObjectMapper, IndexMessageSender messageSender, int defaultBatchSize) {

    this.server = server;
    this.queueWriter = queueWriter;
    this.defaultBatchSize = defaultBatchSize;
    this.bulkSender = new BulkSender(jsonFactory, JsonConfig.Include.NON_EMPTY, defaultObjectMapper, messageSender);
  }

  @Override
  public DocStoreTransaction createTransaction(int batchSize) {
    try {
      return new BulkTransaction(createBulkUpdate(batchSize));
    } catch (IOException e) {
      throw new PersistenceException("Error creating bulk transaction", e);
    }
  }

  @Override
  public void commit(DocStoreTransaction docStoreTxn) {
    docStoreTxn.flush();
    queue(docStoreTxn.queue());
  }

  private void queue(final DocStoreUpdates changesToQueue) {
    if (changesToQueue != null) {
      server.backgroundExecutor().execute(() -> {
        try {
          logger.log(DEBUG, "queue wait for changes...");
          Thread.sleep(1000);
          process(changesToQueue, 0);
        } catch (Exception e) {
          logger.log(ERROR, "Error processing queued changes ", e);
        }
      });
    }
  }

  /**
   * Initialise communication with the queue.
   */
  public void onStartup() {
    queueWriter.onStartup();
  }

  /**
   * Create an 'update by query' processor.
   */
  @Override
  public <T> DocStoreQueryUpdate<T> createQueryUpdate(BeanType<T> beanType, int batchSize) throws IOException {
    BulkUpdate bulkUpdate = createBulkUpdate(batchSize);
    return new ElasticQueryUpdate<T>(bulkUpdate, beanType);
  }

  /**
   * Create the BulkUpdate for batch sending bulk API messages.
   */
  public BulkUpdate createBulkUpdate(int batchSize) throws IOException {
    int batch = (batchSize > 0) ? batchSize : defaultBatchSize;
    return new BulkUpdate(batch, bulkSender);
  }

  /**
   * Process the post-commit updates that have come from the Ebean transaction manager.
   */
  @Override
  public void process(DocStoreUpdates updates, int batchSize) {

    try {
      BulkUpdate txn = createBulkUpdate(batchSize);

      for (DocStoreUpdate persistEvent : updates.persistEvents()) {
        persistEvent.docStoreUpdate(txn.obtain());
      }
      for (DocStoreUpdate deleteEvent : updates.deleteEvents()) {
        deleteEvent.docStoreUpdate(txn.obtain());
      }

      processQueue(txn, updates.nestedEvents());
      txn.flush();

      sendQueueEvents(updates);

    } catch (IOException e) {
      //TODO: updates to queue entries and insert into queue
      logger.log(ERROR, "Failed to send bulk updates", e);
    }
  }

  /**
   * Process queue entries.
   */
  public long processQueue(BulkUpdate txn, List<DocStoreQueueEntry> entries) throws IOException {

    long count = 0;

    Collection<UpdateGroup> groups = ConvertToGroups.groupByQueueId(entries);

    for (UpdateGroup group : groups) {
      BeanType<?> desc = server.beanTypeForQueueId(group.getQueueId());
      count += ProcessGroup.process(server, desc, group, txn);
    }

    return count;
  }

  /**
   * Add the queue entries to the queue for later processing.
   */
  private void sendQueueEvents(DocStoreUpdates docStoreUpdates) {
    queueWriter.queue(docStoreUpdates.queueEntries());
  }

}
