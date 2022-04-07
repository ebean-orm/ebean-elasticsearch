package integration;

import io.ebean.DB;
import io.ebean.FetchConfig;
import org.example.domain.Contact;
import org.example.domain.Customer;
import org.example.domain.Order;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

public class QueryJoinTest extends BaseTest {

  @Test
  public void contacts_to_customer() {

    List<Contact> contacts = server.find(Contact.class)
        .setUseDocStore(true)
        .findList();

    for (Contact contact : contacts) {

      Customer customer = contact.getCustomer();
      // invoke lazy loading
      customer.getWhenCreated();
    }

    String json = DB.json().toJson(contacts);
    System.out.println(json);
  }


  @Test
  public void customer_to_orders() {

    List<Customer> customers = server.find(Customer.class)
        .setUseDocStore(true)
        .findList();

    for (Customer customer : customers) {
      List<Order> orders = customer.getOrders();
      orders.size();
    }

    String json = DB.json().toJson(customers);
    System.out.println(json);
    assertThat(json).contains("\"orders\":[{\"id\":");
  }

  @Test
  public void customer_queryJoin() {

    List<Customer> customers = server.find(Customer.class)
        .setUseDocStore(true)
        .fetchQuery("orders")
        .fetchQuery("contacts")
        .fetchQuery("orders.shipments")
        .findList();

    String json = DB.json().toJson(customers);
    System.out.println(json);

    assertThat(json).contains("\"orders\":[{\"id\":");
    assertThat(json).contains("\"contacts\":[{\"id\":");
    assertThat(json).contains("\"shipments\":[{\"id\":");
  }

  @Test
  public void orders_queryJoinDB_shipments() {

    List<Order> orders = server.find(Order.class)
        .setUseDocStore(true)
        .fetch("shipments", FetchConfig.ofQuery())
        .findList();

    String json = DB.json().toJson(orders);
    System.out.println(json);
    assertThat(json).contains("\"shipments\":[{\"id\":");
  }

  @Test
  public void orders_lazyJoinDB_shipments() {

    List<Order> orders = server.find(Order.class)
        .setUseDocStore(true)
        .findList();

    for (Order order : orders) {
      order.getShipments().size();
    }

    String json = DB.json().toJson(orders);
    System.out.println(json);
    assertThat(json).contains("\"shipments\":[{\"id\":");
  }

  @Test
  public void findEach_with_queryJoinToIndex() {

    final List<Customer> collect = new ArrayList<>();

    server.find(Customer.class)
        .setUseDocStore(true)
        .setMaxRows(2) // reduce size to 2 such that we get multiple scrolls
        .fetch("orders", FetchConfig.ofQuery())
        .fetch("contacts", FetchConfig.ofQuery())
        .findEach(bean -> collect.add(bean));

    String json = DB.json().toJson(collect);
    System.out.println(json);
    assertThat(json).contains("\"contacts\":[{\"id\":");
    assertThat(json).contains("\"orders\":[{\"id\":");
  }

  @Test
  public void orders_lazyJoinDB_when_DisableLazyLoadingTRUE_expect_noShipments() {

    List<Order> orders = server.find(Order.class)
        .setDisableLazyLoading(true)
        .setUseDocStore(true)
        .findList();

    for (Order order : orders) {
      assertThat(order.getShipments().size()).isEqualTo(0);
    }

    String json = DB.json().toJson(orders);
    System.out.println(json);
    assertThat(json).doesNotContain("\"shipments\":[{\"id\":");
  }
}
