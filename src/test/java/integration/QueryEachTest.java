package integration;

import com.avaje.ebean.Query;
import com.avaje.ebean.QueryEachConsumer;
import com.avaje.ebean.QueryEachWhileConsumer;
import org.example.domain.Product;
import org.testng.annotations.Test;

import java.util.LinkedHashSet;
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
}
