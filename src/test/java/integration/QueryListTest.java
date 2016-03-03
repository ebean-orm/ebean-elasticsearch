package integration;

import com.avaje.ebean.Query;
import org.example.domain.Customer;
import org.example.domain.Product;
import org.testng.annotations.Test;

import java.sql.Timestamp;
import java.util.List;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class QueryListTest extends BaseTest {

  @Test
  public void findAll() {

    Query<Product> query = server.find(Product.class).setUseDocStore(true);

    List<Product> products = query.findList();

    assertTrue(!products.isEmpty());
    assertEquals(query.getGeneratedSql(), "{\"query\":{\"match_all\":{}}}");
  }

  @Test
  public void where_startsWith_product() {

    Query<Product> query = server.find(Product.class)
        .setUseDocStore(true)
        .where().startsWith("sku", "C00")
        .query();

    List<Product> products = query.findList();

    assertEquals(products.size(), 3);
    assertEquals(query.getGeneratedSql(), "{\"query\":{\"filtered\":{\"filter\":{\"prefix\":{\"sku\":\"c00\"}}}}}");
  }

  @Test
  public void where_contains_product() {

    Query<Product> query = server.find(Product.class)
        .setUseDocStore(true)
        .where().contains("sku", "C00")
        .query();

    List<Product> products = query.findList();

    assertEquals(products.size(), 3);
    assertEquals(query.getGeneratedSql(), "{\"query\":{\"filtered\":{\"filter\":{\"wildcard\":{\"sku\":\"*c00*\"}}}}}");
  }

  @Test
  public void where_endsWith_product() {

    Query<Product> query = server.find(Product.class)
        .setUseDocStore(true)
        .where().endsWith("sku", "1")
        .query();

    List<Product> products = query.findList();

    assertEquals(products.size(), 2);
    assertEquals(query.getGeneratedSql(), "{\"query\":{\"filtered\":{\"filter\":{\"wildcard\":{\"sku\":\"*1\"}}}}}");
  }

  @Test
  public void where_like_product() {

    Query<Product> query = server.find(Product.class)
        .setUseDocStore(true)
        .where().like("sku", "C_0%")
        .query();

    List<Product> products = query.findList();

    assertEquals(products.size(), 3);
    assertEquals(query.getGeneratedSql(), "{\"query\":{\"filtered\":{\"filter\":{\"wildcard\":{\"sku\":\"c?0*\"}}}}}");
  }

  @Test
  public void where_ieq_product() {

    Query<Product> query = server.find(Product.class)
        .setUseDocStore(true)
        .where().ieq("name", "chair")
        .query();

    List<Product> products = query.findList();

    assertEquals(products.size(), 1);
    assertEquals(query.getGeneratedSql(), "{\"query\":{\"filtered\":{\"filter\":{\"match\":{\"name\":\"chair\"}}}}}");
  }

  @Test
  public void where_ieq_when_hasSpaces() {

    Query<Customer> query = server.find(Customer.class)
        .setUseDocStore(true)
        .where().ieq("name", "cust noaddress")
        .query();

    List<Customer> customers = query.findList();

    assertEquals(customers.size(), 1);
    assertEquals(query.getGeneratedSql(), "{\"query\":{\"filtered\":{\"filter\":{\"bool\":{\"must\":[{\"match\":{\"name\":\"cust\"}},{\"match\":{\"name\":\"noaddress\"}}]}}}}}");
  }

  @Test
  public void where_eq() {

    Query<Customer> query = server.find(Customer.class)
        .setUseDocStore(true)
        .where().eq("name", "Rob")
        .query();

    List<Customer> customers = query.findList();

    assertEquals(customers.size(), 1);
    assertEquals(query.getGeneratedSql(), "{\"query\":{\"filtered\":{\"filter\":{\"term\":{\"name.raw\":\"Rob\"}}}}}");
  }

  @Test
  public void where_in() {

    Query<Customer> query = server.find(Customer.class)
        .setUseDocStore(true)
        .where().in("name", "Rob", "Junk")
        .query();

    List<Customer> customers = query.findList();

    assertEquals(customers.size(), 1);
    assertEquals(query.getGeneratedSql(), "{\"query\":{\"filtered\":{\"filter\":{\"terms\":{\"name.raw\":[\"Rob\",\"Junk\"]}}}}}");
  }

  @Test
  public void where_between_onRaw() {

    Query<Customer> query = server.find(Customer.class)
        .setUseDocStore(true)
        .where().between("name", "R", "S")
        .query();

    List<Customer> customers = query.findList();

    assertEquals(customers.size(), 1);
    assertEquals(query.getGeneratedSql(), "{\"query\":{\"filtered\":{\"filter\":{\"range\":{\"name.raw\":{\"gte\":\"R\",\"lte\":\"S\"}}}}}}");
  }

  @Test
  public void where_between_dateTime() {

    Timestamp before = new Timestamp(System.currentTimeMillis() - 1000000);
    Timestamp now = new Timestamp(System.currentTimeMillis() - 1000000);

    Query<Customer> query = server.find(Customer.class)
        .setUseDocStore(true)
        .where().between("anniversary", before, now)
        .query();

    List<Customer> customers = query.findList();

    assertEquals(customers.size(), 1);
    assertEquals(query.getGeneratedSql(), "{\"query\":{\"filtered\":{\"filter\":{\"range\":{\"name.raw\":{\"gte\":\"R\",\"lte\":\"S\"}}}}}}");
  }
}
