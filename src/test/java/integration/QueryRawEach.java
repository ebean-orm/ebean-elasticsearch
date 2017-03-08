package integration;

import io.ebeanservice.docstore.api.RawDoc;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

public class QueryRawEach extends BaseTest {

  @Test
  public void findEach_startsWith_product() {

    String rawQuery = "{\"query\":{\"filtered\":{\"filter\":{\"prefix\":{\"sku\":\"c00\"}}}}}";

    List<RawDoc> docs = new ArrayList<>();
    AtomicInteger count = new AtomicInteger();

    // perform a scroll query
    server.docStore().findEach("product", rawQuery, rawDoc -> {
      docs.add(rawDoc);
      count.incrementAndGet();
    });

    assertThat(count.get()).isGreaterThan(1);
    assertThat(count.get()).isEqualTo(docs.size());
  }

  @Test
  public void findEachWhile_startsWith_product() {

    String rawQuery = "{\"size\":5,\"query\":{\"match_all\":{}}}";

    List<RawDoc> docs = new ArrayList<>();
    AtomicInteger count = new AtomicInteger();

    // perform a scroll query
    server.docStore().findEachWhile("product", rawQuery, rawDoc -> {
      docs.add(rawDoc);
      return count.incrementAndGet() < 3;
    });

    assertThat(count.get()).isGreaterThan(2);
    assertThat(count.get()).isEqualTo(docs.size());
  }
}
