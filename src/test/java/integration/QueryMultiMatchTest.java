package integration;

import io.ebean.Query;
import io.ebean.search.MultiMatch;
import org.example.domain.Customer;
import org.testng.annotations.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.testng.Assert.assertEquals;

public class QueryMultiMatchTest extends BaseTest {

  @Test
  public void multiMatch() {

    Query<Customer> query = server.find(Customer.class)
        .text()
        .multiMatch("Rob", "name", "smallNotes")
        .query();

    List<Customer> list = query.findList();
    assertThat(list).hasSize(1);
    assertEquals(query.getGeneratedSql(), "{\"track_total_hits\":true,\"query\":{\"multi_match\":{\"query\":\"Rob\",\"fields\":[\"name\",\"smallNotes\"]}}}");
  }

  @Test
  public void multiMatch_with_options() {

    MultiMatch match = MultiMatch.fields("name", "smallNotes").boost(2).opAnd();

    Query<Customer> query = server.find(Customer.class)
        .text()
        .multiMatch("Rob", match)
        .query();

    List<Customer> list = query.findList();
    assertThat(list).hasSize(1);
    assertEquals(query.getGeneratedSql(), "{\"track_total_hits\":true,\"query\":{\"multi_match\":{\"query\":\"Rob\",\"fields\":[\"name\",\"smallNotes\"],\"operator\":\"and\",\"boost\":2.0}}}");
  }

  @Test
  public void multiMatch_with_allOptions() {

    MultiMatch match = MultiMatch.fields("name", "smallNotes")
        .opAnd()
        .boost(2)
        .minShouldMatch("1")
        .analyzer("whitespace")
        //.cutoffFrequency(2)
        .maxExpansions(10)
        .tieBreaker(0.3)
        .type(MultiMatch.Type.CROSS_FIELDS)
        .zeroTerms("all");

    Query<Customer> query = server.find(Customer.class)
        .text()
        .multiMatch("Rob", match)
        .query();

    List<Customer> list = query.findList();

    assertEquals(query.getGeneratedSql(), "{\"track_total_hits\":true,\"query\":{\"multi_match\":{\"query\":\"Rob\",\"fields\":[\"name\",\"smallNotes\"],\"type\":\"cross_fields\",\"tie_breaker\":0.3,\"max_expansions\":10,\"operator\":\"and\",\"boost\":2.0,\"minimum_should_match\":\"1\",\"zero_terms_query\":\"all\",\"analyzer\":\"whitespace\"}}}");
    assertThat(list).hasSize(0);
  }

}
