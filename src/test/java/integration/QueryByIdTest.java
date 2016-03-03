package integration;

import org.example.domain.Product;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

public class QueryByIdTest extends BaseTest {

  @Test
  public void setId_product() {

    Product product = server.find(Product.class)
        .setId(1)
        .setUseDocStore(true)
        .findUnique();

    assertNotNull(product);
    assertEquals(product.getId(), Long.valueOf(1));
  }

  @Test
  public void idEq_product() {

    Product product = server.find(Product.class)
        .where().idEq(1)
        .setUseDocStore(true)
        .findUnique();

    assertNotNull(product);
    assertEquals(product.getId(), Long.valueOf(1));
  }

}
