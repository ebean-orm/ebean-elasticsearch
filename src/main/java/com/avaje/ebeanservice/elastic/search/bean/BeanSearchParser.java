package com.avaje.ebeanservice.elastic.search.bean;

import com.avaje.ebean.plugin.BeanType;
import com.avaje.ebean.text.json.EJson;
import com.avaje.ebean.text.json.JsonBeanReader;
import com.avaje.ebeaninternal.server.deploy.BeanPropertyAssocMany;
import com.avaje.ebeanservice.elastic.search.BaseSearchResultParser;
import com.fasterxml.jackson.core.JsonParser;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 */
public class BeanSearchParser<T> extends BaseSearchResultParser {

  private final BeanSourceReader<T> listener;

  private Map<String, Object> fields;

  public BeanSearchParser(JsonParser parser, BeanType<T> desc, JsonBeanReader<T> reader, BeanPropertyAssocMany<?> lazyLoadMany) {
    super(parser);
    this.listener = new BeanSourceReader<T>(desc, reader, lazyLoadMany);
  }

  public BeanSearchParser(JsonParser parser, BeanSearchParser<T> source) {
    super(parser);
    this.listener = source.listener;
  }


  public BeanSearchParser<T> moreJson(JsonParser parser) {
    this.listener.moreJson(parser);
    return new BeanSearchParser<T>(parser, this);
  }

  /**
   * Return true if all the hits have been read.
   */
  public boolean allHitsRead() {
    return total == 0 || total == listener.size();
  }

  /**
   * Return true if the total hits is zero.
   */
  public boolean zeroHits() {
    return listener.size() == 0;
  }

  /**
   * Return the JSON returning the list of beans.
   */
  public List<T> read() throws IOException {

    readAll();
    return listener.getList();
  }

  public void readSource() throws IOException {
    listener.readSource(parser, id);
  }

  public void readFields() throws IOException {
    fields = EJson.parseObject(parser);
    listener.readFields(fields, id, score);
  }

  @Override
  public void readIdOnly() {
    listener.readIdOnly(id, score);
  }
}
