package org.example.integrationtests;

import com.avaje.ebean.Ebean;
import com.avaje.ebean.PagedList;
import com.avaje.ebean.Query;
import integration.BaseTest;
import integration.support.SeedDbData;
import org.example.domain.Country;
import org.example.domain.Order;
import org.example.domain.Product;
import org.testng.annotations.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.testng.Assert.assertNotNull;

@Test(enabled = false)
public class ProductTest extends BaseTest {

  private void indexCountries() {

    Query<Country> query = Ebean.find(Country.class)
        .where().query();

    Ebean.getDefaultServer().docStore().indexByQuery(query);

  }

  public void testDb() throws InterruptedException {

    //EmbeddedElasticServer server = new EmbeddedElasticServer();

    SeedDbData.reset(true);

    PagedList<Product> products =
        server.find(Product.class)
        .setUseDocStore(true)
        .where().contains("name", "chair")
        .setMaxRows(10)
        .findPagedList();



    Query<Order> query = Ebean.find(Order.class)
        .where().gt("id", 1)
        .query();

    Ebean.getDefaultServer().docStore().indexByQuery(query);


    Thread.sleep(2000);

    Order order = Ebean.getDefaultServer().docStore().getById(Order.class, 2);

    assertNotNull(order);
    assertThat(order.getId()).isEqualTo(2);
  }
}