package integration;

import io.ebean.Query;
import org.example.domain.Customer;
import org.example.domain.Order;
import org.testng.annotations.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.testng.Assert.assertEquals;

public class QueryMatchTest extends BaseTest {

  @Test
  public void match_oneExpression_expect_noBoolWrapper() {

    Query<Customer> query = server.find(Customer.class)
        .text()
        .match("name", "Rob")
        .setUseDocStore(true);

    List<Customer> list = query.findList();
    assertThat(list).hasSize(1);
    assertEquals(query.getGeneratedSql(), "{\"query\":{\"match\":{\"name\":\"Rob\"}}}");
  }

  @Test
  public void match_multipleExpression_expect_boolShould() {

    Query<Customer> query = server.find(Customer.class)
        .text()
        .match("name", "Rob")
        .match("smallNote", "interesting")
        .setUseDocStore(true);

    List<Customer> list = query.findList();
    assertThat(list).hasSize(1);
    assertEquals(query.getGeneratedSql(), "{\"query\":{\"bool\":{\"should\":[{\"match\":{\"name\":\"Rob\"}},{\"match\":{\"smallNote\":\"interesting\"}}]}}}");
  }

  @Test
  public void explicitMust_WithMultipleExpression_expect_boolMust() {

    Query<Customer> query = server.find(Customer.class)
        .text()
        .must()
          .match("name", "Rob")
          .match("smallNote", "interesting")
        .query();

    List<Customer> list = query.findList();
    assertEquals(query.getGeneratedSql(), "{\"query\":{\"bool\":{\"must\":[{\"match\":{\"name\":\"Rob\"}},{\"match\":{\"smallNote\":\"interesting\"}}]}}}");
    assertThat(list).hasSize(0);
  }

  @Test
  public void mustAndShould() {

    Query<Customer> query = server.find(Customer.class)
        .text()
        .must()
          .match("name", "Rob")
          .eq("status", Customer.Status.NEW)
          .endJunction()
        .should()
          .match("smallNote", "foo")
          .match("smallNote", "bar")
        .setUseDocStore(true);

    List<Customer> list = query.findList();
    assertEquals(query.getGeneratedSql(), "{\"query\":{\"bool\":{\"must\":[{\"match\":{\"name\":\"Rob\"}},{\"term\":{\"status\":\"NEW\"}}],\"should\":[{\"match\":{\"smallNote\":\"foo\"}},{\"match\":{\"smallNote\":\"bar\"}}]}}}");
    assertThat(list).hasSize(1);
  }

  @Test
  public void must_withNestedShould() {

    Query<Customer> query = server.find(Customer.class)
        .text()
        .must()
          .match("name", "Rob")
          .should()
            .match("smallNote", "foo")
            .match("smallNote", "bar")
            .setUseDocStore(true);

    List<Customer> list = query.findList();
    assertEquals(query.getGeneratedSql(), "{\"query\":{\"bool\":{\"must\":[{\"match\":{\"name\":\"Rob\"}},{\"bool\":{\"should\":[{\"match\":{\"smallNote\":\"foo\"}},{\"match\":{\"smallNote\":\"bar\"}}]}}]}}}");
    assertThat(list).hasSize(0);
  }

  @Test
  public void textSearchAndFilter() {
    Query<Order> query = server.find(Order.class)
        .text()
        .must()
          .match("customer.name", "Rob")
        .where()
          .eq("status", Order.Status.COMPLETE)
          .query();

    List<Order> list = query.findList();
    assertEquals(query.getGeneratedSql(), "{\"query\":{\"bool\":{\"must\":[{\"match\":{\"customer.name\":\"Rob\"}}]}},\"filter\":{\"term\":{\"status\":\"COMPLETE\"}}}");
    assertThat(list).hasSize(1);
  }

  @Test
  public void mustMatchAndTerm_expect_noFilter() {
    Query<Order> query = server.find(Order.class)
        .text()
        .must()
          .match("customer.name", "Rob")
          .eq("status", Order.Status.COMPLETE)
        .query();

    List<Order> list = query.findList();
    assertEquals(query.getGeneratedSql(), "{\"query\":{\"bool\":{\"must\":[{\"match\":{\"customer.name\":\"Rob\"}},{\"term\":{\"status\":\"COMPLETE\"}}]}}}");
    assertThat(list).hasSize(1);
  }
}
