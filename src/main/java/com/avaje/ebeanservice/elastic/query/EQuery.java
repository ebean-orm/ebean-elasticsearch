package com.avaje.ebeanservice.elastic.query;

import com.avaje.ebean.plugin.BeanType;
import com.avaje.ebean.text.json.JsonBeanReader;
import com.avaje.ebean.text.json.JsonContext;
import com.avaje.ebean.text.json.JsonReadOptions;
import com.avaje.ebeaninternal.api.SpiQuery;
import com.avaje.ebeanservice.elastic.search.bean.BeanSearchParser;
import com.fasterxml.jackson.core.JsonParser;

/**
 * Class for processing query requests.
 */
public class EQuery<T> {

  protected final SpiQuery<T> query;

  protected final BeanType<T> beanType;

  protected final JsonContext jsonContext;

  protected final JsonReadOptions jsonOptions;

  public EQuery(SpiQuery<T> query, JsonContext jsonContext, JsonReadOptions jsonOptions) {
    this.query = query;
    this.beanType = query.getBeanDescriptor();
    this.jsonContext = jsonContext;
    this.jsonOptions = jsonOptions;
  }

  public EQuery(BeanType<T> beanType, JsonContext jsonContext, JsonReadOptions options) {
    this.query = null;
    this.beanType = beanType;
    this.jsonContext = jsonContext;
    this.jsonOptions = options;
  }

  /**
   * Create a bean parser for the given json.
   */
  protected BeanSearchParser<T> createParser(JsonParser json) {
    JsonBeanReader reader = createReader(json);
    return createParser(json, reader);
  }

  /**
   * Create a bean reader for the given json.
   */
  public JsonBeanReader<T> createReader(JsonParser json) {
    return jsonContext.createBeanReader(beanType, json, jsonOptions);
  }

  private BeanSearchParser<T> createParser(JsonParser json, JsonBeanReader<T> reader) {
    return new BeanSearchParser<T>(json, beanType, reader, query.getLazyLoadMany());
  }

}
