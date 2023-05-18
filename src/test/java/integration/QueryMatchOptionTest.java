package integration;

import io.ebean.Query;
import io.ebean.search.Match;
import org.example.domain.Customer;
import org.testng.annotations.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.testng.Assert.assertEquals;

public class QueryMatchOptionTest extends BaseTest {

  @Test
  public void matchOr() {

    Query<Customer> query = server.find(Customer.class)
        .text()
        .match("name", "Cust NoAddress", new Match().opOr())
        .query();

    List<Customer> list = query.findList();
    assertThat(list).hasSize(1);
    assertEquals(query.getGeneratedSql(), "{\"track_total_hits\":true,\"query\":{\"match\":{\"name\":{\"query\":\"Cust NoAddress\"}}}}");
  }

  @Test
  public void matchAnd() {

    Query<Customer> query = server.find(Customer.class)
        .text()
        .match("name", "Cust NoAddress", new Match().opAnd())
        .query();

    List<Customer> list = query.findList();
    assertEquals(query.getGeneratedSql(), "{\"track_total_hits\":true,\"query\":{\"match\":{\"name\":{\"query\":\"Cust NoAddress\",\"operator\":\"and\"}}}}");
    //Review assertThat(list).hasSize(1);
  }


  @Test
  public void matchAnd_when_doesNotExist_expect_noHits() {

    Query<Customer> query = server.find(Customer.class)
        .text()
        .match("name", "Cust DoesNotExist", new Match().opAnd())
        .query();

    List<Customer> list = query.findList();
    assertThat(list).hasSize(0);
  }

  @Test
  public void matchAllPhraseOptions() {

    Match options = new Match().phrase()
        .analyzer("whitespace")
        //.boost(2)
        //.cutoffFrequency(1)
        //.minShouldMatch("50%")
        .zeroTerms("all")
        .maxExpansions(3) // maxExpansions is for phrasePrefix only
        .phrase();

    Query<Customer> query = server.find(Customer.class)
        .text()
        .match("name", "Cust DoesNotExist", options)
        .query();

    List<Customer> list = query.findList();
    assertEquals(query.getGeneratedSql(), "{\"track_total_hits\":true,\"query\":{\"match_phrase\":{\"name\":{\"query\":\"Cust DoesNotExist\",\"zero_terms_query\":\"all\",\"analyzer\":\"whitespace\"}}}}");
    assertThat(list).hasSize(0);
  }

  @Test
  public void matchAllPhrasePrefixOptions() {

    Match options = new Match().phrase()
        .analyzer("whitespace")
        //.boost(2)
        //.cutoffFrequency(1)
        //.minShouldMatch("50%")
        .maxExpansions(3)
        .phrasePrefix();

    Query<Customer> query = server.find(Customer.class)
        .text()
        .match("name", "Cust NoAdd", options)
        .query();

    List<Customer> list = query.findList();
    assertEquals(query.getGeneratedSql(), "{\"track_total_hits\":true,\"query\":{\"match_phrase_prefix\":{\"name\":{\"query\":\"Cust NoAdd\",\"analyzer\":\"whitespace\",\"max_expansions\":3}}}}");
    assertThat(list).hasSize(0);
  }

  @Test
  public void matchPhrasePrefix() {

    Match options = new Match().phrase().phrasePrefix();

    Query<Customer> query = server.find(Customer.class)
        .text()
        .match("name", "Cust NoAdd", options)
        .query();

    List<Customer> list = query.findList();
    assertEquals(query.getGeneratedSql(), "{\"track_total_hits\":true,\"query\":{\"match_phrase_prefix\":{\"name\":{\"query\":\"Cust NoAdd\"}}}}");
    // Review assertThat(list).hasSize(1);
  }
}
