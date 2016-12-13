package integration;

import io.ebean.Query;
import org.example.domain.Product;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

public class QueryUniqueTest extends BaseTest {

  @Test
  public void unique_product_sku() {

    Query<Product> query = server.find(Product.class)
        .where().eq("sku", "C001")
        .setUseDocStore(true);

    Product product = query.findUnique();

    assertEquals(query.getGeneratedSql(), "{\"query\":{\"filtered\":{\"filter\":{\"term\":{\"sku.raw\":\"C001\"}}}}}");
    assertNotNull(product);
    assertEquals(product.getId(), Long.valueOf(1));
    assertEquals(product.getSku(), "C001");
  }

  // FAILS
  @Test(enabled = false)
  public void eq_findUnique_product() {

    // Need to translate "id" = 1 ... to idEq(1)
    // ... as "term":{"id":1} does not work obviously

    Product product = server.find(Product.class)
        .where()
        .eq("id", 1)
        .setUseDocStore(true)
        .findUnique();

    assertNotNull(product);
    assertEquals(product.getId(), Long.valueOf(1));
  }
}
