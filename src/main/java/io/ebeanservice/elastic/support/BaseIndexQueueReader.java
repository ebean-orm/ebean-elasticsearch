package io.ebeanservice.elastic.support;

import io.ebean.SqlQuery;
import io.ebean.SqlRow;
import io.ebean.SqlUpdate;
import io.ebean.Transaction;
import io.ebeaninternal.api.SpiEbeanServer;
import io.ebeanservice.docstore.api.DocStoreUpdates;
import io.ebeaninternal.server.deploy.BeanDescriptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * Base implementation that will periodically read the queue and process the entries.
 */
public class BaseIndexQueueReader {

  protected Logger logger = LoggerFactory.getLogger(BaseIndexQueueReader.class);

  protected final SpiEbeanServer server;

  protected final String queueTableName;

  protected final String sqlObtainEntries;

  protected final String markProcessingSql;

  public BaseIndexQueueReader(SpiEbeanServer server, String queueTableName){
    this.server = server;
    this.queueTableName = queueTableName;
    this.sqlObtainEntries = getObtainSql();
    this.markProcessingSql = getMarkProcessingSql();
  }

  public boolean process() {

    // obtain a cluster wide lock
    if (!obtainClusterWideLock()) {
      logger.debug("did not obtain cluster wide lock");
      return false;
    }

    List<SqlRow> entries = obtainEntriesReleaseLock();


    DocStoreUpdates docStoreUpdates = new DocStoreUpdates();

    // group by queueId/type
    Map<String,List<SqlRow>> map = groupEntriesByQueueId(entries);

    for (Map.Entry<String, List<SqlRow>> queueEntries : map.entrySet()) {
      addEntries(docStoreUpdates, queueEntries);
    }


    // loop the entries and process them via bulk api
    // successful entries delete
    // unsuccessful entries re-activate

    return true;
  }

  private void addEntries(DocStoreUpdates docStoreUpdates, Map.Entry<String, List<SqlRow>> queueEntries) {

    String queueId = queueEntries.getKey();
    List<SqlRow> entries = queueEntries.getValue();

    // convert into a query

  }

  /**
   * Return the entries grouped by queueId.
   */
  private Map<String, List<SqlRow>> groupEntriesByQueueId(List<SqlRow> entries) {

    Map<String, List<SqlRow>> map = new LinkedHashMap<String, List<SqlRow>>();

    for (int i = 0; i < entries.size(); i++) {

      SqlRow entry = entries.get(i);
      String queueId = entry.getString("queue_id");

      List<SqlRow> list = map.get(queueId);
      if (list == null) {
        // no map entry for this queueId so initialise it
        list = new ArrayList<SqlRow>();
        map.put(queueId, list);
      }
      list.add(entry);
    }

    return map;
  }

  private void addEntry(DocStoreUpdates docStoreUpdates, SqlRow entry) {

    BeanDescriptor<?> desc = null;
    //desc.
    //indexUpdates.add();

  }

  private List<SqlRow> obtainEntriesReleaseLock() {
    try {

      // read a batch of queue entries
      Transaction transaction = server.createTransaction();
      try {
        SqlQuery sqlQuery = server.sqlQuery(sqlObtainEntries);
        List<SqlRow> rows = server.findList(sqlQuery, transaction);
        SqlUpdate sqlUpdate = server.sqlUpdate(markProcessingSql);

        transaction.setBatchSize(100);
        // update the entries marking them as processing
        for (SqlRow row : rows) {
          markEntryAsProcessing(row, sqlUpdate, transaction);
        }

        // return the entries
        transaction.commit();

        return rows;

      } finally {
        transaction.end();
      }

    } finally {
      releaseClusterWideLock();
    }

  }

  protected void markEntryAsProcessing(SqlRow row, SqlUpdate sqlUpdate, Transaction transaction) {

    String docId = row.getString("doc_id");
    sqlUpdate.setParameter(1, docId);
    sqlUpdate.execute();

  }


  protected void releaseClusterWideLock() {


  }

  protected boolean obtainClusterWideLock() {

    return true;
  }

  protected String getMarkProcessingSql() {

    return "update "+queueTableName
        +" set processing = "+ BaseIndexQueueWriter.PROCESSING_TRUE
        +" where id = ?";
  }

  protected String getObtainSql() {

    return "select id, queue_id, doc_id, action, processing, when_queued from "
        + queueTableName
        + " where processing = "+ BaseIndexQueueWriter.PROCESSING_FALSE
        +" order by id";


  }
}
