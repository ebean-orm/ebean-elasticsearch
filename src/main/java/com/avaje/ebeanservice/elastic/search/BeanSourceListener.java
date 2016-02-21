package com.avaje.ebeanservice.elastic.search;

import com.avaje.ebean.plugin.SpiBeanType;
import com.avaje.ebean.plugin.SpiExpressionPath;
import com.fasterxml.jackson.core.JsonParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 */
public class BeanSourceListener<T> implements SearchSourceListener {

  final SpiBeanType<T> desc;

  List<T> beans = new ArrayList<T>();

  T currentBean;

  public BeanSourceListener(SpiBeanType<T> desc) {
    this.desc = desc;
  }


  @Override
  public void readSource(JsonParser parser, String id) throws IOException {
    currentBean = desc.jsonRead(parser, null, null);
    desc.setBeanId(currentBean, id);
    beans.add(currentBean);
  }

  @Override
  public void readFields(Map<String, Object> fields, String id, double score) {

    if (currentBean != null) {
      applyFields(currentBean, fields);

    } else {
      T bean = desc.createBean();
      desc.setBeanId(bean, id);
      applyFields(bean, fields);
      beans.add(bean);
    }

  }

  private void applyFields(T bean, Map<String, Object> fields) {

    Set<Map.Entry<String, Object>> entries = fields.entrySet();
    for (Map.Entry<String, Object> entry : entries) {
      SpiExpressionPath path = desc.expressionPath(entry.getKey());
      List<Object> value = (List<Object>)entry.getValue();

      if (!path.containsMany()) {
        if (value.size() == 1) {
          path.set(bean, value.get(0));
        }
      }
    }
  }

  public List<T> getList() {
    return beans;
  }

  public int size() {
    return beans.size();
  }

}
