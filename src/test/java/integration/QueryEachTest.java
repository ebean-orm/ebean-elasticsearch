package integration;

import com.avaje.ebean.Ebean;
import com.avaje.ebean.FetchConfig;
import com.avaje.ebean.Query;
import com.avaje.ebean.QueryEachConsumer;
import com.avaje.ebean.QueryEachWhileConsumer;
import org.example.domain.Order;
import org.example.domain.Product;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.testng.Assert.assertEquals;

public class QueryEachTest extends BaseTest {

  @Test
  public void findEach_startsWith_product() {

    Query<Product> query = server.find(Product.class)
        .setUseDocStore(true)
        .where().startsWith("sku", "C00")
        .query();

    final LinkedHashSet<String> skuSet = new LinkedHashSet<String>();
    final AtomicInteger count = new AtomicInteger();

    query.findEach(new QueryEachConsumer<Product>() {
      @Override
      public void accept(Product bean) {
        count.incrementAndGet();
        skuSet.add(bean.getSku());
      }
    });

    assertEquals(count.get(), 3);
    assertEquals(skuSet.size(), 3);
    assertEquals(query.getGeneratedSql(), "{\"query\":{\"filtered\":{\"filter\":{\"prefix\":{\"sku\":\"c00\"}}}}}");
  }

  @Test
  public void findEachWhile_startsWith_product() {

    Query<Product> query = server.find(Product.class)
        .setUseDocStore(true)
        .where().startsWith("sku", "C00")
        .query();

    final LinkedHashSet<String> skuSet = new LinkedHashSet<String>();
    final AtomicInteger count = new AtomicInteger();

    query.findEachWhile(new QueryEachWhileConsumer<Product>() {
      @Override
      public boolean accept(Product bean) {
        skuSet.add(bean.getSku());
        return count.incrementAndGet() < 2;
      }
    });

    assertEquals(count.get(), 2);
    assertEquals(skuSet.size(), 2);
    assertEquals(query.getGeneratedSql(), "{\"query\":{\"filtered\":{\"filter\":{\"prefix\":{\"sku\":\"c00\"}}}}}");
  }

  @Test
  public void findEach_join() {

    Query<Order> query = server.find(Order.class)
        .setUseDocStore(true)
        .setMaxRows(2)
        .select("id, customer.id")
        .fetch("customer", new FetchConfig().query())
        .fetch("shipments", new FetchConfig().query())
        .orderBy().asc("whenCreated");

    final List<Order> collect = new ArrayList<Order>();

    query.findEach(new QueryEachConsumer<Order>() {
      @Override
      public void accept(Order bean) {
        collect.add(bean);
      }
    });

    String json = Ebean.json().toJson(collect);
    System.out.println(json);

    for (Order order : collect) {

      System.out.println("order id:"+order.getId()+" shipments:"+order.getShipments().size());
    }
  }
}
