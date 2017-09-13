package integration;

import io.ebean.DocumentStore;
import io.ebean.Query;
import org.example.domain.Product;
import org.testng.annotations.Test;

import java.sql.Timestamp;

import static org.testng.Assert.assertEquals;

public class IndexCopyTest extends BaseTest {

  @Test
  public void indexByQuery() {

    long since = 1457045750115L;

    Query<Product> query = server.find(Product.class)
        .where()
        .ge("whenModified", new Timestamp(since))
        .startsWith("sku", "C")
        .query();

    server.docStore().indexByQuery(query, 1000);
  }


  @Test
  public void copyIndex() {

    DocumentStore documentStore = server.docStore();

    documentStore.dropIndex("product_copy");
    documentStore.createIndex("product_copy", null);

    long docCount = documentStore.copyIndex(Product.class, "product_copy");

    int dbRowCount = server.find(Product.class).findCount();

    assertEquals(docCount, dbRowCount);
  }


  @Test
  public void copyIndexSince() {

    DocumentStore documentStore = server.docStore();

    documentStore.dropIndex("product_copy");
    documentStore.createIndex("product_copy", null);

    long since = 1457045750115L;
    long docCount = documentStore.copyIndex(Product.class, "product_copy", since);

    int dbRowCount = server.find(Product.class)
        .where().ge("whenModified", new Timestamp(since))
        .findCount();

    assertEquals(docCount, dbRowCount);
  }


  @Test
  public void copyIndexSince_inFuture() {

    DocumentStore documentStore = server.docStore();

    documentStore.dropIndex("product_copy");
    documentStore.createIndex("product_copy", null);

    long since = 9457045750115L;
    long docCount = documentStore.copyIndex(Product.class, "product_copy", since);

    assertEquals(docCount, 0);
  }

  @Test
  public void copyIndex_usingQuery() {

    DocumentStore documentStore = server.docStore();

    documentStore.dropIndex("product_copy");
    documentStore.createIndex("product_copy", null);

    long since = 1457045750115L;

    Query<Product> query = server.find(Product.class)
        .where()
        .ge("whenModified", new Timestamp(since))
        .istartsWith("sku", "c")
        .query();

    long docCount = documentStore.copyIndex(query, "product_copy", 0);

    assertEquals(docCount, query.findCount());
  }

  @Test
  public void copyIndex_usingQuery_withBatchSize() {

    DocumentStore documentStore = server.docStore();

    documentStore.dropIndex("product_copy");
    documentStore.createIndex("product_copy", null);

    long since = 1457045750115L;

    Query<Product> query = server.find(Product.class)
        .where()
        .ge("whenModified", new Timestamp(since))
        .query();

    long docCount = documentStore.copyIndex(query, "product_copy", 2);

    assertEquals(docCount, query.findCount());
  }
}
