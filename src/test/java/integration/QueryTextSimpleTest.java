package integration;

import io.ebean.Query;
import io.ebean.search.TextSimple;
import org.example.domain.Customer;
import org.testng.annotations.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.testng.Assert.assertEquals;

public class QueryTextSimpleTest extends BaseTest {

  @Test
  public void noOptions() {

    TextSimple options = new TextSimple();

    Query<Customer> query = server.find(Customer.class)
        .text()
        .textSimple("quick brown", options)
        .query();

    List<Customer> list = query.findList();
    assertThat(list).hasSize(0);
    assertEquals(query.getGeneratedSql(), "{\"track_total_hits\":true,\"query\":{\"simple_query_string\":{\"query\":\"quick brown\"}}}");
  }

  @Test
  public void flags() {

    TextSimple options = new TextSimple()
        .flags("OR|AND|PREFIX");

    Query<Customer> query = server.find(Customer.class)
        .text()
        .textSimple("quick brown", options)
        .query();

    List<Customer> list = query.findList();
    assertThat(list).hasSize(0);
    assertEquals(query.getGeneratedSql(), "{\"track_total_hits\":true,\"query\":{\"simple_query_string\":{\"query\":\"quick brown\",\"flags\":\"OR|AND|PREFIX\"}}}");
  }

  @Test
  public void allOptions() {

    TextSimple options = new TextSimple()
        .analyzer("whitespace")
        .analyzeWildcard(true)
        .fields("name")
        .lenient(true)
        //.locale("EN")
        //.lowercaseExpandedTerms(false)
        .minShouldMatch("1")
        .opAnd();

    Query<Customer> query = server.find(Customer.class)
        .text()
        .textSimple("quick brown", options)
        .query();

    List<Customer> list = query.findList();
    assertThat(list).hasSize(0);
    assertEquals(query.getGeneratedSql(), "{\"track_total_hits\":true,\"query\":{\"simple_query_string\":{\"query\":\"quick brown\",\"analyzer\":\"whitespace\",\"fields\":[\"name\"],\"default_operator\":\"and\",\"analyze_wildcard\":true,\"lenient\":true,\"minimum_should_match\":\"1\"}}}");
  }


}
