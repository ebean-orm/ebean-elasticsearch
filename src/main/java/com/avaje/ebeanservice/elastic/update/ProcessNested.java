package com.avaje.ebeanservice.elastic.update;

import com.avaje.ebean.EbeanServer;
import com.avaje.ebean.FetchPath;
import com.avaje.ebean.Query;
import com.avaje.ebean.QueryEachConsumer;
import com.avaje.ebean.plugin.BeanDocType;
import com.avaje.ebean.plugin.BeanType;
import com.avaje.ebean.plugin.Property;
import com.avaje.ebeaninternal.server.query.SplitName;
import com.avaje.ebeanservice.elastic.bulk.BulkUpdate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Process an embedded document update.
 */
public class ProcessNested<T> {

  private static final Logger log = LoggerFactory.getLogger(ProcessNested.class);

  private final EbeanServer server;
  private final BeanType<T> desc;
  private final BulkUpdate txn;
  private final UpdateNested nested;

  private final Map<Object, String> jsonMap = new HashMap<Object,String>();

  private final String nestedPath;
  private final String nestedIdProperty;
  private final String fullNestedPath;
  private final FetchPath nestedDoc;
  private final BeanType<?> nestedDesc;

  private final Property nestedProperty;
  private final String selectId;
  private final String nestedIdPropertyName;
  private final boolean nestedMany;
  private final BeanDocType<T> beanDocType;
  private FetchPath manyRootDoc;

  private long count;

  public ProcessNested(EbeanServer server, BeanType<T> desc, BulkUpdate txn, UpdateNested nested) {
    this.server = server;
    this.desc = desc;
    this.txn = txn;
    this.nested = nested;
    this.fullNestedPath = nested.getPath();

    beanDocType = desc.docStore();

    String[] nestedPathSplit = getTopNestedPath(fullNestedPath);
    this.nestedPath = nestedPathSplit[0];
    this.nestedIdProperty = nestedPathSplit[1];

    this.nestedDoc = beanDocType.getEmbedded(this.nestedPath);
    this.nestedDesc = desc.getBeanTypeAtPath(this.nestedPath);

    this.nestedProperty = desc.getProperty(nestedPath);
    this.nestedMany = nestedProperty.isMany();
    if (nestedMany) {
      manyRootDoc = beanDocType.getEmbeddedManyRoot(nestedPath);
    }

    this.selectId = createSelectId(desc);
    this.nestedIdPropertyName = nestedDesc.getIdProperty().getName();
  }

  private String createSelectId(BeanType<T> desc) {
    String id =desc.getIdProperty().getName();
    if (desc.hasInheritance()) {
      id += ","+desc.getDiscColumn();
    }
    return id;
  }

  /**
   * Return the 'top' part of the nested path.
   */
  private String[] getTopNestedPath(String fullNestedPath) {
    return SplitName.splitBegin(fullNestedPath);
  }

  public long process() throws IOException {

    List<Object> nestedIds = nested.getIds();
    if (nestedMany) {
      fetchEmbeddedAssocMany(nestedIds);
      processTop(nestedIds);
    } else {
      updateByQueryAssocOne(nestedIds);
    }

    return count;
  }

  /**
   * Load the json map given the embedded document has cardinality one (ElasticSearch object).
   */
  private void updateByQueryAssocOne(List<Object> nestedIds) {

    Query<?> pathQuery = server.createQuery(nestedDesc.getBeanType());
    pathQuery.apply(nestedDoc);
    pathQuery.where().in(nestedIdProperty, nestedIds);

    // hit the database and build the embedded JSON documents
    List<?> list = pathQuery.findList();
    for (Object bean : list) {
      String embedJson = server.json().toJson(bean, nestedDoc);
      Object beanId = nestedDesc.beanId(bean);
      jsonMap.put(beanId, embedJson);

      String script =
      "{ \"query\":{\"bool\":{\"filter\":{\"term\":{\""+fullNestedPath+"\":\""+beanId+"\"}}}}" +
      " ,\"script\": { \"lang\": \"painless\", "+
      "  \"inline\": \"ctx._source."+nestedPath+" = params."+nestedPath+"\",  "+
      "  \"params\" : { "+
      "  \""+nestedPath+"\":" + embedJson +
      "}}}";

      BeanDocType<T> docType = desc.docStore();
      try {
        txn.sendUpdateQuery(docType.getIndexName(), docType.getIndexType(), script);
      } catch (IOException e) {
        log.error("Error performing updateByQuery", e);
      }
    }
  }

  /**
   * Load the json map given the embedded document has cardinality many (ElasticSearch nested).
   */
  private void fetchEmbeddedAssocMany(List<Object> nestedIds) {

    Query<T> query = server.createQuery(desc.getBeanType());
    query.apply(manyRootDoc);
    query.where().in(fullNestedPath, nestedIds);

    // hit the database and build the embedded JSON documents
    List<T> list = query.findList();
    for (T bean : list) {
      Object manyList = nestedProperty.getVal(bean);
      String embedJson = server.json().toJson(manyList, nestedDoc);
      Object beanId = desc.beanId(bean);
      jsonMap.put(beanId, embedJson);
    }
  }

  protected void processTop(List<Object> nestedIds) {

    Query<T> topQuery = server.createQuery(desc.getBeanType());
    topQuery.setUseDocStore(true);
    topQuery.select(selectId);
    if (!nestedMany) {
      topQuery.fetch(nestedPath, nestedIdPropertyName);
    }
    topQuery.where().in(fullNestedPath, nestedIds);

    // elasticSearch scroll query
    topQuery.findEach(new QueryEachConsumer<T>() {
      @Override
      public void accept(T bean)  {
        updateEmbedded(bean);
        count++;
      }
    });
  }

  private void updateEmbedded(T bean) {
    try {
      Object beanId = desc.getBeanId(bean);
      Object targetId;
      if (nestedMany) {
        targetId = beanId;
      } else {
        Object embBean = nestedProperty.getVal(bean);
        targetId = nestedDesc.beanId(embBean);
      }

      String json = jsonMap.get(targetId);
      beanDocType.updateEmbedded(beanId, nestedPath, json, txn.obtain());

    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }


}
