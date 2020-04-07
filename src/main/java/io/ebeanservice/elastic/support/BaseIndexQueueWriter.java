package io.ebeanservice.elastic.support;

import io.ebean.DocStoreQueueEntry;
import io.ebean.EbeanServer;
import io.ebean.SqlUpdate;
import io.ebean.Transaction;

import java.sql.Timestamp;
import java.util.List;

/**
 * Base implementation of IndexQueueWriter that inserts the events into a database table.
 */
public class BaseIndexQueueWriter implements IndexQueueWriter {

  public static final int PROCESSING_FALSE = 0;

  public static final int PROCESSING_TRUE = 1;

  final EbeanServer server;

  final String sql;

  public BaseIndexQueueWriter(EbeanServer server, String tableName) {
    this.server = server;
    this.sql = createSql(tableName);
  }

  protected String createSql(String tableName) {
    return "insert into "+tableName+" (queue_id, doc_id, action, path, processing, when_queued) values (?,?,?,?,?)";
  }

  @Override
  public void onStartup() {
    // check queue connectivity
  }

  @Override
  public void queue(List<DocStoreQueueEntry> queueEntries) {

    if (true) {
      return;
    }
    if (queueEntries.isEmpty()) {
      return;
    }

    SqlUpdate sqlUpdate = server.sqlUpdate(sql);
    Transaction transaction = server.createTransaction();
    try {
      transaction.setBatchSize(100);

      for (DocStoreQueueEntry entry : queueEntries) {
        sqlUpdate.setParameter(1, entry.getQueueId());
        sqlUpdate.setParameter(2, entry.getBeanId().toString());
        sqlUpdate.setParameter(3, entry.getType().getValue());
        sqlUpdate.setParameter(4, entry.getPath());
        sqlUpdate.setParameter(5, PROCESSING_FALSE);
        sqlUpdate.setParameter(6, new Timestamp(System.currentTimeMillis()));

        sqlUpdate.execute();
      }

      transaction.commit();

    } finally {
      transaction.end();
    }
  }
}
