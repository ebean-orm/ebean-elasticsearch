package com.avaje.ebeanservice.elastic.bulk;

import com.avaje.ebeanservice.docstore.api.DocStoreTransaction;
import com.avaje.ebeanservice.docstore.api.DocStoreUpdateContext;
import com.avaje.ebeanservice.docstore.api.DocStoreUpdates;

public class BulkTransaction implements DocStoreTransaction {

  private final BulkUpdate bulkUpdate;

  private DocStoreUpdates queueUpdates;

  public BulkTransaction(BulkUpdate bulkUpdate) {
    this.bulkUpdate = bulkUpdate;
  }

  @Override
  public DocStoreUpdateContext obtain() {
    return bulkUpdate.obtain();
  }

  @Override
  public DocStoreUpdates queue() {
    if (queueUpdates == null) {
      queueUpdates = new DocStoreUpdates();
    }
    return queueUpdates;
  }

  @Override
  public void flush() {
    bulkUpdate.flush();
  }
}
