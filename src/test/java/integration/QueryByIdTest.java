package integration;

import io.ebean.FetchConfig;
import org.example.domain.Customer;
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
        .findOne();

    assertNotNull(product);
    assertEquals(product.getId(), Long.valueOf(1));
  }

  @Test
  public void eqId_product() {

    Product product = server.find(Product.class)
        .where().eq("id", 1)
        .setUseDocStore(true)
        .findOne();

    assertNotNull(product);
    assertEquals(product.getId(), Long.valueOf(1));
  }

  @Test
  public void idEq_product() {

    Product product = server.find(Product.class)
        .where().idEq(1)
        .setUseDocStore(true)
        .findOne();

    assertNotNull(product);
    assertEquals(product.getId(), Long.valueOf(1));
  }


  @Test
  public void customer_withQueryJoin() {

    Customer customer = server.find(Customer.class)
        .fetch("contacts", FetchConfig.ofQuery())
        .where()
        .idEq(1)
        .setUseDocStore(true)
        .findOne();

    assertNotNull(customer);
    assertNotNull(customer.getContacts());
  }

  @Test
  public void customer_withLazy() {

    Customer customer = server.find(Customer.class)
        .where()
        .idEq(1)
        .setUseDocStore(true)
        .findOne();

    customer.getContacts().size();
    assertNotNull(customer);
  }

  @Test
  public void customer_viaDocStore_withLazy() {

    Customer customer = server.find(Customer.class)
        .setId(1).setUseDocStore(true)
        .findOne();

    customer.getContacts().size();
    assertNotNull(customer);
  }
}
