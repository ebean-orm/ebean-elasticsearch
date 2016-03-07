package integration;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.node.Node;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;

@Test
public class NativeClientQueryTest extends BaseTest {

  public void nativeQuery() throws InterruptedException {

    String rawJson = "{\"size\":10,\"query\":{\"filtered\":{\"filter\":{\"match\":{\"name\":\"chair\"}}}}}";

    Node node = embeddedServer.getNode();

    if (node == null) {
      // do not run without embedded server
      return;
    }

    Client client = node.client();
    SearchResponse response = client.prepareSearch("product")
        .setSource(rawJson)
        .execute()
        .actionGet();

    SearchHits hits = response.getHits();
    SearchHit[] hits1 = hits.hits();

    assertEquals(hits1.length, 1);
  }
}