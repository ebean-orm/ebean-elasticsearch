package io.ebeanservice.elastic.testdoubles;

import io.ebeanservice.docstore.api.DocStoreUpdate;
import io.ebeanservice.docstore.api.DocStoreUpdateContext;
import io.ebeanservice.docstore.api.DocStoreUpdates;

import java.io.IOException;

public class TDDocStoreUpdate implements DocStoreUpdate {

  public DocStoreUpdateContext txn;

  public DocStoreUpdates docStoreUpdates;

  @Override
  public void docStoreUpdate(DocStoreUpdateContext txn) throws IOException {
    this.txn = txn;
  }

  @Override
  public void addToQueue(DocStoreUpdates docStoreUpdates) {
    this.docStoreUpdates = docStoreUpdates;
  }
}
