package integration;

import com.avaje.ebean.Query;
import com.avaje.ebean.search.TextQueryString;
import org.example.domain.Customer;
import org.testng.annotations.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.testng.Assert.assertEquals;

public class QueryTextQueryStringTest extends BaseTest {

  @Test
  public void noOptions() {

    TextQueryString options = new TextQueryString();

    Query<Customer> query = server.find(Customer.class)
        .text()
        .textQueryString("quick brown", options)
        .query();

    List<Customer> list = query.findList();
    assertThat(list).hasSize(0);
    assertEquals(query.getGeneratedSql(), "{\"query\":{\"query_string\":{\"query\":\"quick brown\"}}}");
  }

  @Test
  public void findRob() {

    TextQueryString options = new TextQueryString();

    Query<Customer> query = server.find(Customer.class)
        .text()
        .textQueryString("Rob", options)
        .query();

    List<Customer> list = query.findList();
    assertEquals(query.getGeneratedSql(), "{\"query\":{\"query_string\":{\"query\":\"Rob\"}}}");
    assertThat(list).hasSize(1);
  }

  @Test
  public void find_plusCust_plusNew() {

    TextQueryString options = new TextQueryString();

    Query<Customer> query = server.find(Customer.class)
        .text()
        .textQueryString("+Cust +New", options)
        .query();

    List<Customer> list = query.findList();
    assertEquals(query.getGeneratedSql(), "{\"query\":{\"query_string\":{\"query\":\"+Cust +New\"}}}");
    assertThat(list).hasSize(1);
  }

  @Test
  public void find_plusCust_minusNew() {

    TextQueryString options = new TextQueryString();

    Query<Customer> query = server.find(Customer.class)
        .text()
        .textQueryString("+Cust -New", options)
        .query();

    List<Customer> list = query.findList();
    assertEquals(query.getGeneratedSql(), "{\"query\":{\"query_string\":{\"query\":\"+Cust -New\"}}}");
    assertThat(list).hasSize(0);
  }


  @Test
  public void allOptions() {

    TextQueryString options = new TextQueryString()
        .allowLeadingWildcard(false)
        .analyzer("whitespace")
        .analyzeWildcard(true)
        .autoGeneratePhraseQueries(true)
        .phraseSlop(0.5)
        .boost(2)
        .defaultField("name")
        .enablePositionIncrements(false)
        .fuzziness("1")
        .fuzzyMaxExpansions(10)
        .fuzzyPrefixLength(3)
        .lenient(true)
        .locale("EN")
        .lowercaseExpandedTerms(false)
        .maxDeterminizedStates(500)
        .minShouldMatch("1")
        .timeZone("UTC")
        .opAnd();

    Query<Customer> query = server.find(Customer.class)
        .text()
        .textQueryString("brown", options)
        .query();


    List<Customer> list = query.findList();
    assertEquals(query.getGeneratedSql(), "{\"query\":{\"query_string\":{\"query\":\"brown\",\"default_field\":\"name\",\"default_operator\":\"and\",\"analyzer\":\"whitespace\",\"allow_leading_wildcard\":false,\"lowercase_expanded_terms\":false,\"enable_position_increments\":false,\"fuzzy_max_expansions\":10,\"fuzziness\":\"1\",\"fuzzy_prefix_length\":3,\"phrase_slop\":0.5,\"boost\":2.0,\"analyze_wildcard\":true,\"auto_generate_phrase_queries\":true,\"max_determinized_states\":500,\"minimum_should_match\":\"1\",\"lenient\":true,\"locale\":\"EN\",\"time_zone\":\"UTC\"}}}");
    assertThat(list).hasSize(0);
  }

}
