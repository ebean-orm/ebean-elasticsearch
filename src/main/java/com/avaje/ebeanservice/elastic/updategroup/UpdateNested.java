package com.avaje.ebeanservice.elastic.updategroup;

import java.util.ArrayList;
import java.util.List;

/**
 *
 */
public class UpdateNested {

  private final String path;

  private final List<Object> ids = new ArrayList<Object>();

  public UpdateNested(String path) {
    this.path = path;
  }

  public String getPath() {
    return path;
  }

  public List<Object> getIds() {
    return ids;
  }

  public void addId(Object id) {
    ids.add(id);
  }
}
