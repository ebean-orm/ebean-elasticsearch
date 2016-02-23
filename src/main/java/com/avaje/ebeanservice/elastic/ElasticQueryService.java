package com.avaje.ebeanservice.elastic;

import com.avaje.ebean.PersistenceIOException;
import com.avaje.ebean.Query;
import com.avaje.ebean.QueryEachConsumer;
import com.avaje.ebean.plugin.SpiBeanType;
import com.avaje.ebean.plugin.SpiServer;
import com.avaje.ebeanservice.docstore.api.DocumentNotFoundException;
import com.avaje.ebeanservice.elastic.search.SearchResultParser;
import com.avaje.ebeanservice.elastic.support.IndexMessageResponse;
import com.avaje.ebeanservice.elastic.support.IndexMessageSender;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 *
 */
public class ElasticQueryService {

  private static final Logger logger = LoggerFactory.getLogger(ElasticDocumentStore.class);

  final SpiServer server;

  final JsonFactory jsonFactory;

  final IndexMessageSender messageSender;

  public ElasticQueryService(SpiServer server, JsonFactory jsonFactory, IndexMessageSender messageSender) {
    this.server = server;
    this.jsonFactory = jsonFactory;
    this.messageSender = messageSender;
  }

  public <T> List<T> findList(Query<T> query) {

    Class<T> beanType = query.getBeanType();
    SpiBeanType<T> desc = server.getBeanType(beanType);

    try {
      JsonParser jp = postQuery(false, desc.getDocStoreIndexType(), desc.getDocStoreIndexName(), query.asElasticQuery());

      SearchResultParser<T> resultParser = new SearchResultParser<T>(jp, desc);
      return resultParser.read();

    } catch (IOException e) {
      throw new PersistenceIOException(e);
    }

  }


  private JsonParser postQuery(boolean scroll, String indexType, String indexName, String jsonQuery) throws IOException, DocumentNotFoundException {


    IndexMessageResponse response = messageSender.postQuery(scroll, indexType, indexName, jsonQuery);
    switch (response.getCode()) {
      case 404:
        throw new DocumentNotFoundException("404 for query?");
      case 200:
        return jsonFactory.createParser(response.getBody());
      default:
        throw new IOException("Unhandled response code " + response.getCode() + " body:" + response.getBody());
    }
  }

  private JsonParser getSource(String indexType, String indexName, Object docId) throws IOException, DocumentNotFoundException {

    IndexMessageResponse response = messageSender.getDocSource(indexType, indexName, docId.toString());
    switch (response.getCode()) {
      case 404:
        throw new DocumentNotFoundException("404 for docId:" + docId);
      case 200:
        return jsonFactory.createParser(response.getBody());
      default:
        throw new IOException("Unhandled response code " + response.getCode() + " body:" + response.getBody());
    }
  }

  private JsonParser getScroll(String scrollId) throws IOException, DocumentNotFoundException {

    IndexMessageResponse response = messageSender.getScroll(scrollId);
    switch (response.getCode()) {
      case 404:
        throw new DocumentNotFoundException("404 for scrollId:" + scrollId);
      case 200:
        return jsonFactory.createParser(response.getBody());
      default:
        throw new IOException("Unhandled response code " + response.getCode() + " body:" + response.getBody());
    }
  }

  public <T> void findEach(Query<T> query, QueryEachConsumer<T> consumer) {

    Class<T> beanType = query.getBeanType();
    SpiBeanType<T> desc = server.getBeanType(beanType);
    Set<String> scrollIds = new LinkedHashSet<String>();
    try {
      JsonParser initialJson = postQuery(true, desc.getDocStoreIndexType(), desc.getDocStoreIndexName(), query.asElasticQuery());
      SearchResultParser<T> initialParser = new SearchResultParser<T>(initialJson, desc);

      List<T> list = initialParser.read();
      for (T bean : list) {
        consumer.accept(bean);
      }

      String scrollId = initialParser.getScrollId();
      scrollIds.add(scrollId);

      if (!initialParser.allHitsRead()) {
        while (true) {
          JsonParser moreJson = getScroll(scrollId);
          SearchResultParser<T> moreParser = new SearchResultParser<T>(moreJson, desc);

          List<T> moreList = moreParser.read();
          for (T bean : moreList) {
            consumer.accept(bean);
          }

          scrollId = moreParser.getScrollId();
          scrollIds.add(scrollId);

          if (moreParser.zeroHits()) {
            break;
          }
        }
      }

    } catch (IOException e) {
      throw new PersistenceIOException(e);

    } finally {
      clearScrollIds(scrollIds);
    }

  }

  public <T> T getById(Class<T> beanType, Object id) {
    SpiBeanType<T> desc = server.getBeanType(beanType);
    if (desc == null) {
      throw new IllegalArgumentException("Type [" + beanType + "] does not appear to be an entity bean type?");
    }

    try {
      JsonParser parser = getSource(desc.getDocStoreIndexType(), desc.getDocStoreIndexName(), id);
      T bean = desc.jsonRead(parser, null, null);
      desc.setBeanId(bean, id);

      return bean;

    } catch (DocumentNotFoundException e) {
      // this is treated like findUnique() so returning null
      return null;

    } catch (IOException e) {
      throw new PersistenceIOException(e);
    }
  }


  private void clearScrollIds(Set<String> scrollIds) {
    try {
      messageSender.clearScrollIds(scrollIds);
    } catch (IOException e) {
      logger.error("Error trying to clear scrollIds: "+scrollIds, e);
    }
  }
}
