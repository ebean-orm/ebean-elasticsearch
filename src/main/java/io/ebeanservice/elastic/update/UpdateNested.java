package io.ebeanservice.elastic.update;

import java.util.ArrayList;
import java.util.List;

/**
 * A group of nested updates for a given path.
 * <p>
 * We group these together to process in batch/bulk.
 * </p>
 */
public class UpdateNested {

  private final String path;

  private final List<Object> ids = new ArrayList<Object>();

  /**
   * Construct given the path.
   */
  public UpdateNested(String path) {
    this.path = path;
  }

  /**
   * Return the path.
   */
  public String getPath() {
    return path;
  }

  /**
   * Return the Ids.
   */
  public List<Object> getIds() {
    return ids;
  }

  /**
   * Add an Id to the list.
   */
  public void addId(Object id) {
    ids.add(id);
  }
}
