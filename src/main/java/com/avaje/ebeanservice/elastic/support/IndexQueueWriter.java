package com.avaje.ebeanservice.elastic.support;

import com.avaje.ebean.DocStoreQueueEntry;

import java.util.List;

/**
 * Pushes queue entries onto a queue for future processing.
 */
public interface IndexQueueWriter {

  /**
   * Push all the queue entries onto the queue.
   */
  void queue(List<DocStoreQueueEntry> queueEntries);
}
