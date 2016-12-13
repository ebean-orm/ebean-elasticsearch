package io.ebeanservice.elastic.bulk;

import io.ebeanservice.docstore.api.DocStoreTransaction;
import io.ebeanservice.docstore.api.DocStoreUpdateContext;
import io.ebeanservice.docstore.api.DocStoreUpdates;

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
