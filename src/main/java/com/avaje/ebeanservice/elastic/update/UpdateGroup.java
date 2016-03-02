package com.avaje.ebeanservice.elastic.update;

import com.avaje.ebean.DocStoreQueueEntry;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Groups index update events by queueId.
 *
 * Some nested path updates can overlap with index events so it is good to process these together as
 * a group and check for these overlaps (and skip unnecessary work).
 */
public class UpdateGroup {

  private final String queueId;

  private final List<Object> deleteIds = new ArrayList<Object>();

  private final List<Object> indexIds = new ArrayList<Object>();

  private final Map<String, UpdateNested> pathIds = new LinkedHashMap<String, UpdateNested>();

  protected UpdateGroup(String queueId) {
    this.queueId = queueId;
  }

  public String getQueueId() {
    return queueId;
  }

  public List<Object> getDeleteIds() {
    return deleteIds;
  }

  public List<Object> getIndexIds() {
    return indexIds;
  }

  public Map<String, UpdateNested> getNestedPathIds() {
    return pathIds;
  }

  private void addIndex(Object id) {
    indexIds.add(id);
  }

  private void addDelete(Object id) {
    deleteIds.add(id);
  }

  private void addNested(String path, Object beanId) {
    UpdateNested nested = pathIds.get(path);
    if (nested == null) {
      nested = new UpdateNested(path);
      pathIds.put(path, nested);
    }
    nested.addId(beanId);
  }

  protected void addEntry(DocStoreQueueEntry entry) {

    DocStoreQueueEntry.Action type = entry.getType();
    switch (type) {
      case DELETE:
        addDelete(entry.getBeanId());
        break;
      case INDEX:
        addIndex(entry.getBeanId());
        break;
      case NESTED:
        addNested(entry.getPath(), entry.getBeanId());
        break;
      default:
        throw new IllegalArgumentException("type " + type + " not handled");
    }

  }

}
