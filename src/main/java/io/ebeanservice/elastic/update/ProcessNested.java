package io.ebeanservice.elastic.update;

import io.ebean.*;
import io.ebean.plugin.BeanDocType;
import io.ebean.plugin.BeanType;
import io.ebean.plugin.Property;
import io.ebean.util.SplitName;
import io.ebeanservice.elastic.bulk.BulkUpdate;
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

  private final Database server;
  private final BeanType<T> desc;
  private final BulkUpdate txn;
  private final UpdateNested nested;

  private final Map<Object, String> jsonMap = new HashMap<>();

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

  public ProcessNested(Database server, BeanType<T> desc, BulkUpdate txn, UpdateNested nested) {
    this.server = server;
    this.desc = desc;
    this.txn = txn;
    this.nested = nested;
    this.fullNestedPath = nested.getPath();

    beanDocType = desc.docStore();

    String[] nestedPathSplit = getTopNestedPath(fullNestedPath);
    this.nestedPath = nestedPathSplit[0];
    this.nestedIdProperty = nestedPathSplit[1];

    this.nestedDoc = beanDocType.embedded(this.nestedPath);
    this.nestedDesc = desc.beanTypeAtPath(this.nestedPath);

    this.nestedProperty = desc.property(nestedPath);
    this.nestedMany = nestedProperty.isMany();
    if (nestedMany) {
      manyRootDoc = beanDocType.embeddedManyRoot(nestedPath);
    }

    this.selectId = createSelectId(desc);
    this.nestedIdPropertyName = nestedDesc.idProperty().name();
  }

  private String createSelectId(BeanType<T> desc) {
    String id = desc.idProperty().name();
    if (desc.hasInheritance()) {
      id += "," + desc.discColumn();
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
      return updateByQueryAssocOne(nestedIds);
    }

    return count;
  }

  /**
   * Load the json map given the embedded document has cardinality one (ElasticSearch object).
   */
  private long updateByQueryAssocOne(List<Object> nestedIds) {

    Query<?> pathQuery = server.createQuery(nestedDesc.type());
    pathQuery.apply(nestedDoc);
    pathQuery.where().in(nestedIdProperty, nestedIds);

    // hit the database and build the embedded JSON documents
    List<?> list = pathQuery.findList();
    for (Object bean : list) {
      String embedJson = server.json().toJson(bean, nestedDoc);
      Object beanId = nestedDesc.id(bean);
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
        Map<String, Object> response = txn.sendUpdateQuery(docType.indexName(), docType.indexType(), script);

        Object updatedDocs = response.get("total");
        if (updatedDocs instanceof Number) {
          return ((Number)updatedDocs).longValue();
        }

      } catch (IOException e) {
        log.error("Error performing updateByQuery", e);
      }
    }
    return 0;
  }

  /**
   * Load the json map given the embedded document has cardinality many (ElasticSearch nested).
   */
  private void fetchEmbeddedAssocMany(List<Object> nestedIds) {

    Query<T> query = server.createQuery(desc.type());
    query.apply(manyRootDoc);
    query.where().in(fullNestedPath, nestedIds);

    // hit the database and build the embedded JSON documents
    List<T> list = query.findList();
    for (T bean : list) {
      Object manyList = nestedProperty.value(bean);
      String embedJson = server.json().toJson(manyList, nestedDoc);
      Object beanId = desc.id(bean);
      jsonMap.put(beanId, embedJson);
    }
  }

  protected void processTop(List<Object> nestedIds) {

    Query<T> topQuery = server.createQuery(desc.type());
    topQuery.setUseDocStore(true);
    topQuery.select(selectId);
    if (!nestedMany) {
      topQuery.fetch(nestedPath, nestedIdPropertyName);
    }
    topQuery.where().in(fullNestedPath, nestedIds);

    topQuery.findEach(bean -> {
      updateEmbedded(bean);
      count++;
    });
  }

  private void updateEmbedded(T bean) {
    try {
      Object beanId = desc.id(bean);
      Object targetId;
      if (nestedMany) {
        targetId = beanId;
      } else {
        Object embBean = nestedProperty.value(bean);
        targetId = nestedDesc.id(embBean);
      }

      String json = jsonMap.get(targetId);
      if (json == null) {
        log.error("No content for updateEmbedded path:{} id:{}", nestedPath, beanId);
      } else {
        beanDocType.updateEmbedded(beanId, nestedPath, json, txn.obtain());
      }

    } catch (IOException e) {
      throw new PersistenceIOException(e);
    }
  }


}
