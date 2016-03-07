package com.avaje.ebeanservice.elastic.query;

import com.avaje.ebean.plugin.BeanType;
import com.avaje.ebean.text.json.JsonBeanReader;
import com.avaje.ebean.text.json.JsonContext;
import com.avaje.ebean.text.json.JsonReadOptions;
import com.avaje.ebeaninternal.api.SpiQuery;
import com.avaje.ebeanservice.elastic.search.bean.BeanSearchParser;
import com.fasterxml.jackson.core.JsonParser;

/**
 * Base class for query requests.
 */
public class EQuery<T> {

  protected final SpiQuery<T> query;

  protected final BeanType<T> beanType;

  protected final JsonContext jsonContext;

  protected final JsonReadOptions jsonOptions;

  public EQuery(SpiQuery<T> query, JsonContext jsonContext) {
    this.query = query;
    this.beanType = query.getBeanDescriptor();
    this.jsonContext = jsonContext;

    this.jsonOptions = getJsonReadOptions(query);
  }

  public EQuery(BeanType<T> beanType, JsonContext jsonContext, JsonReadOptions options) {
    this.query = null;
    this.beanType = beanType;
    this.jsonContext = jsonContext;
    this.jsonOptions = options;
  }

  /**
   * Return the JsonReadOptions taking into account lazy loading and persistence context.
   */
  protected JsonReadOptions getJsonReadOptions(SpiQuery<?> query) {

    JsonReadOptions options = new JsonReadOptions();
    if (!query.isDisableLazyLoading()) {
      options.setEnableLazyLoading(true);
    }
    options.setPersistenceContext(query.getPersistenceContext());
    return options;
  }

  /**
   * Create a bean parser for the given json.
   */
  protected BeanSearchParser<T> createParser(JsonParser json) {
    JsonBeanReader reader = createReader(json);
    return createParser(json, reader);
  }

  public JsonBeanReader<T> createReader(JsonParser initialJson) {
    return jsonContext.createBeanReader(beanType, initialJson, jsonOptions);
  }

  private BeanSearchParser<T> createParser(JsonParser initialJson, JsonBeanReader<T> reader) {
    return new BeanSearchParser<T>(initialJson, beanType, reader, query.getLazyLoadMany());
  }

}
