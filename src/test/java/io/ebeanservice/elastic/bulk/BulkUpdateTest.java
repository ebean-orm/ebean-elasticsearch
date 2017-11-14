package io.ebeanservice.elastic.bulk;

import io.ebeanservice.elastic.testdoubles.TDBulkSender;
import io.ebeanservice.elastic.testdoubles.TDDocStoreUpdate;
import org.testng.annotations.Test;

import java.io.IOException;

import static org.testng.Assert.*;


public class BulkUpdateTest {

  TDBulkSender tdBulkSender = new TDBulkSender();

  @Test
  public void send_expect_obtain() throws Exception {

    BulkUpdate bulkUpdate = createBulkUpdate(100);

    TDDocStoreUpdate update = new TDDocStoreUpdate();
    bulkUpdate.send(update);

    assertSame(bulkUpdate.obtain(), update.txn);
  }

  @Test
  public void obtain_when_new() throws Exception {

    BulkUpdate bulkUpdate = createBulkUpdate(100);
    assertNotNull(bulkUpdate.obtain());
  }

  @Test
  public void obtain_when_batchExceeded() throws Exception {

    BulkUpdate bulkUpdate = createBulkUpdate(2);

    BulkBuffer obtain1 = bulkUpdate.obtain();
    BulkBuffer obtain2 = bulkUpdate.obtain();
    assertSame(obtain1, obtain2);

    // exceeded batch size of 2
    BulkBuffer obtain3 = bulkUpdate.obtain();
    BulkBuffer obtain4 = bulkUpdate.obtain();
    assertNotSame(obtain2, obtain3);
    assertSame(obtain3, obtain4);

    // exceeded batch size of 2
    BulkBuffer obtain5 = bulkUpdate.obtain();
    BulkBuffer obtain6 = bulkUpdate.obtain();
    assertNotSame(obtain4, obtain5);
    assertSame(obtain5, obtain6);
  }

  @Test
  public void flush_when_empty() throws Exception {

    tdBulkSender.resetForTesting();

    BulkUpdate bulkUpdate = createBulkUpdate(2);
    bulkUpdate.flush();

    assertNull(tdBulkSender.request);
  }

  @Test
  public void flush_when_notEmpty() throws Exception {

    tdBulkSender.resetForTesting();

    BulkUpdate bulkUpdate = createBulkUpdate(2);
    BulkBuffer buffer = bulkUpdate.obtain();

    bulkUpdate.flush();

    assertSame(buffer, tdBulkSender.request);
  }

  private BulkUpdate createBulkUpdate(int batchSize) throws IOException {
    return new BulkUpdate(batchSize, tdBulkSender);
  }


}