package com.avaje.ebeaninternal.server.querydefn;

import com.avaje.ebean.Ebean;
import com.avaje.ebean.Query;
import com.avaje.ebeaninternal.api.SpiQuery;
import com.avaje.ebeaninternal.server.expression.BaseElasticTest;
import org.example.domain.Customer;
import org.example.domain.Order;
import org.testng.annotations.Test;

import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThat;

public class DefaultOrmQueryElasticTest extends BaseElasticTest {

  @Test
  public void writeElastic_on_SpiExpressionList() throws IOException {

    Query<Order> query = Ebean.find(Order.class)
        .where().eq("customer.name", "Rob")
        .query();

    SpiQuery<Order> spiQuery = (SpiQuery<Order>)query;
    String asJson = asJson(spiQuery);

    assertThat(asJson).isEqualTo("{\"query\":{\"bool\":{\"filter\":{\"term\":{\"customer.name.raw\":\"Rob\"}}}}}");
  }

  @Test
  public void writeElastic() throws IOException {

    Query<Order> query = Ebean.find(Order.class)
        .select("status, customer.name, details.product.id")
        .where().eq("customer.name", "Rob")
        .query();

    SpiQuery<Order> spiQuery = (SpiQuery<Order>)query;
    String asJson = asJson(spiQuery);

    assertThat(asJson).isEqualTo("{\"_source\":{\"includes\":[\"status\",\"customer.name\",\"details.product.id\"]},\"query\":{\"bool\":{\"filter\":{\"term\":{\"customer.name.raw\":\"Rob\"}}}}}");
  }

  @Test
  public void asElasticQuery() throws IOException {

    Query<Order> query = Ebean.find(Order.class)
        .select("status")
        .where().eq("customer.name", "Rob")
        .query();

    String asJson = asJson((SpiQuery<Order>)query);

    assertThat(asJson).isEqualTo("{\"_source\":{\"includes\":[\"status\"]},\"query\":{\"bool\":{\"filter\":{\"term\":{\"customer.name.raw\":\"Rob\"}}}}}");
  }

  @Test
  public void asElasticQuery_firstRowsMaxRows() throws IOException {

    Query<Order> query = Ebean.find(Order.class)
        .select("status")
        .setFirstRow(3)
        .setMaxRows(100)
        .where().eq("customer.name", "Rob")
        .query();

    String asJson = asJson((SpiQuery<Order>)query);

    assertThat(asJson).isEqualTo("{\"from\":3,\"size\":100,\"_source\":{\"includes\":[\"status\"]},\"query\":{\"bool\":{\"filter\":{\"term\":{\"customer.name.raw\":\"Rob\"}}}}}");
  }

  @Test
  public void simpleExpression_on_assocId() throws IOException {

    Customer custOne = Ebean.getReference(Customer.class, 1);

    Query<Order> query = Ebean.find(Order.class)
        .where().eq("customer", custOne)
        .query();

    SpiQuery<Order> spiQuery = (SpiQuery<Order>)query;
    String asJson = asJson(spiQuery);

    assertThat(asJson).isEqualTo("{\"query\":{\"bool\":{\"filter\":{\"term\":{\"customer.id\":1}}}}}");
  }
}