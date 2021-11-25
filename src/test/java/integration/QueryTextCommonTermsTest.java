package integration;

import io.ebean.Query;
import io.ebean.search.TextCommonTerms;
import org.example.domain.Customer;
import org.testng.annotations.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.testng.Assert.assertEquals;

public class QueryTextCommonTermsTest extends BaseTest {

  @Test
  public void simple() {

    TextCommonTerms options = new TextCommonTerms().highFreqOperatorAnd(true);

    Query<Customer> query = server.find(Customer.class)
        .text()
        .textCommonTerms("the quick brown", options)
        .setUseDocStore(true);

    List<Customer> list = query.findList();
    assertThat(list).hasSize(0);
    assertEquals(query.getGeneratedSql(), "{\"track_total_hits\":true,\"query\":{\"common\":{\"body\":{\"query\":\"the quick brown\",\"high_freq_operator\":\"and\"}}}}");
  }

  @Test
  public void minShouldMatch_fullOptions() {

    TextCommonTerms options = new TextCommonTerms()
        .cutoffFrequency(0.001)
        .minShouldMatch("50%")
        .lowFreqOperatorAnd(true)
        .highFreqOperatorAnd(true);

    Query<Customer> query = server.find(Customer.class)
        .text()
        .textCommonTerms("the brown", options)
        .query();

    List<Customer> list = query.findList();
    assertThat(list).hasSize(0);
    assertEquals(query.getGeneratedSql(), "{\"track_total_hits\":true,\"query\":{\"common\":{\"body\":{\"query\":\"the brown\",\"cutoff_frequency\":0.001,\"low_freq_operator\":\"and\",\"high_freq_operator\":\"and\",\"minimum_should_match\":\"50%\"}}}}");
  }

  @Test
  public void minShouldMatch_Low() {

    TextCommonTerms options = new TextCommonTerms()
        .cutoffFrequency(1)
        .minShouldMatchLowFreq("50%")
        .lowFreqOperatorAnd(true)
        .highFreqOperatorAnd(true);

    Query<Customer> query = server.find(Customer.class)
        .text()
        .textCommonTerms("the brown", options)
        .setUseDocStore(true);

    List<Customer> list = query.findList();
    assertThat(list).hasSize(0);
    assertEquals(query.getGeneratedSql(), "{\"track_total_hits\":true,\"query\":{\"common\":{\"body\":{\"query\":\"the brown\",\"cutoff_frequency\":1.0,\"low_freq_operator\":\"and\",\"high_freq_operator\":\"and\",\"minimum_should_match\":{\"low_freq\":\"50%\"}}}}}");
  }

  @Test
  public void minShouldMatch_High() {

    TextCommonTerms options = new TextCommonTerms()
        .cutoffFrequency(1)
        .minShouldMatchHighFreq("50%")
        .lowFreqOperatorAnd(true)
        .highFreqOperatorAnd(true);

    Query<Customer> query = server.find(Customer.class)
        .text()
        .textCommonTerms("the brown", options)
        .setUseDocStore(true);

    List<Customer> list = query.findList();
    assertThat(list).hasSize(0);
    assertEquals(query.getGeneratedSql(), "{\"track_total_hits\":true,\"query\":{\"common\":{\"body\":{\"query\":\"the brown\",\"cutoff_frequency\":1.0,\"low_freq_operator\":\"and\",\"high_freq_operator\":\"and\",\"minimum_should_match\":{\"high_freq\":\"50%\"}}}}}");
  }

  @Test
  public void minShouldMatch_LowAndHigh() {

    TextCommonTerms options = new TextCommonTerms()
        .cutoffFrequency(1)
        .minShouldMatchLowFreq("2")
        .minShouldMatchHighFreq("50%");

    Query<Customer> query = server.find(Customer.class)
        .text()
        .textCommonTerms("the brown", options)
        .setUseDocStore(true);

    List<Customer> list = query.findList();
    assertThat(list).hasSize(0);
    assertEquals(query.getGeneratedSql(), "{\"track_total_hits\":true,\"query\":{\"common\":{\"body\":{\"query\":\"the brown\",\"cutoff_frequency\":1.0,\"minimum_should_match\":{\"low_freq\":\"2\",\"high_freq\":\"50%\"}}}}}");
  }


}
