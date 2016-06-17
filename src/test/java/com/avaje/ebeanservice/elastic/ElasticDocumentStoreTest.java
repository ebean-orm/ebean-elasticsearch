package com.avaje.ebeanservice.elastic;

import com.avaje.ebean.Ebean;
import com.avaje.ebean.Expr;
import com.avaje.ebean.PagedList;
import com.avaje.ebean.Query;
import com.avaje.ebean.QueryEachConsumer;
import integration.BaseTest;
import org.example.domain.Contact;
import org.example.domain.Customer;
import org.example.domain.Order;
import org.example.domain.Product;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.testng.Assert.assertNotNull;

public class ElasticDocumentStoreTest extends BaseTest {


  @Test
  public void indexSettings() throws IOException {

    Map<String,Object> settings = new HashMap<String, Object>();
    settings.put("refresh_interval", "-1");
    docStore.indexSettings("product", settings);

    settings.put("refresh_interval", "1s");
    docStore.indexSettings("product", settings);
  }

  @Test
  public void indexCopyTo() {

    docStore.copyIndex(Product.class, "product_v2");
  }

  @Test
  public void indexCopyTo_since() throws InterruptedException {

    String newIndex = "product_v2";
    docStore.dropIndex(newIndex);
    docStore.createIndex(newIndex, null);

    long startEpochMillis = System.currentTimeMillis();

    docStore.copyIndex(Product.class, newIndex);

    Product prod = Ebean.find(Product.class)
        .where().eq("name", "ZChair")
        .findUnique();

    assertNotNull(prod);
    prod.setSku("Z99A");
    Ebean.save(prod);

    Thread.sleep(3000);

    // copy any changes since startEpochMillis
    docStore.copyIndex(Product.class, newIndex, startEpochMillis);
  }

  @Test
  public void sortBy_when_propertyIsAnalysed() throws InterruptedException {

    Query<Order> query = Ebean.find(Order.class)
        .setUseDocStore(true)
        .orderBy().asc("customer.name");

    List<Order> list = query.findList();
    System.out.print("as" + list);
  }

  @Test
  public void findPagedList() throws InterruptedException {

    Query<Order> query = Ebean.find(Order.class)
        .setUseDocStore(true)
        .where().in("customer.id", 1, 2)
        .setMaxRows(2)
        .orderBy().asc("customer.name");

    PagedList<Order> list = query.findPagedList();

    assertThat(list.getTotalRowCount()).isEqualTo(5);
    assertThat(list.getList()).hasSize(2);
  }

  @Test
  public void query_useDocStore_then_lazyLoadAssocMany() throws InterruptedException {

    List<Customer> customers = Ebean.find(Customer.class)
        .select("*")
        .orderBy().asc("name")
        .setUseDocStore(true)
        //.setDisableLazyLoading(true)
        .findList();

    for (Customer customer : customers) {
      List<Contact> contacts = customer.getContacts();
      for (Contact contact : contacts) {
        contact.getFirstName();
        contact.getLastName();
      }
    }

    assertThat(customers).hasSize(4);
  }

  @Test
  public void query_useDocStore_then_lazyLoadAssocOne() throws InterruptedException {

    List<Contact> contacts = Ebean.find(Contact.class)
        .where().icontains("lastName", "bunny")
        .orderBy().asc("lastName")
        .setUseDocStore(true)
        .findList();

    for (Contact contact : contacts) {
      Customer customer = contact.getCustomer();
      customer.getId();
      customer.getName();
      customer.getSmallNote();
      customer.getId();
    }

    assertThat(contacts).hasSize(1);
  }

  @Test
  public void term_when_propertyIsAnalysed() {

    Query<Order> query = Ebean.find(Order.class)
        .setUseDocStore(true)
        .where().eq("customer.name", "Rob")
        .orderBy().asc("customer.name");

    List<Order> list1 = query.findList();

    Query<Order> query2 = Ebean.find(Order.class)
        .setUseDocStore(true)
        .where().ne("customer.name", "Rob")
        .orderBy().asc("customer.name");

    List<Order> list2 = query2.findList();

    assertThat(list1).hasSize(3);
    assertThat(list2).hasSize(2);
  }

  @Test
  public void in_when() {

    Query<Order> query = Ebean.find(Order.class)
        .setUseDocStore(true)
        .where().in("customer.id", 1, 2)
        .orderBy().asc("customer.name");

    List<Order> list = query.findList();

    assertThat(list).hasSize(5);
  }

  @Test
  public void idsIn_when() {

    List<Integer> ids = Arrays.asList(1, 3);
    Query<Order> query = Ebean.find(Order.class)
        .setUseDocStore(true)
        .where().idIn(ids)
        .orderBy().asc("customer.name");

    List<Order> list = query.findList();

    assertThat(list).hasSize(2);
  }

  @Test
  public void exists_when() {

    Query<Order> query = Ebean.find(Order.class)
        .setUseDocStore(true)
        .where().isNotNull("status").query();

    List<Order> list = query.findList();
    assertThat(list).hasSize(2);
  }

  @Test
  public void exists_when_multipleFields() {

    Query<Order> query = Ebean.find(Order.class)
        .setUseDocStore(true)
        .where().isNotNull("status").isNotNull("customer.id")
        .query();

    List<Order> list = query.findList();
    assertThat(list).hasSize(2);
  }

  @Test
  public void notExists_when() {

    Query<Order> query = Ebean.find(Order.class)
        .setUseDocStore(true)
        .where().isNull("status").query();

    List<Order> list = query.findList();

    assertThat(list).hasSize(3);
  }


  @Test
  public void disjunction_when() {

    Query<Order> query = Ebean.find(Order.class)
        .setUseDocStore(true)
        .where()
        .disjunction().isNotNull("status").gt("customer.id", 1)
        .query();

    List<Order> list = query.findList();
    assertThat(list).hasSize(3);
  }


  @Test
  public void conjunction_when() {

    Query<Order> query = Ebean.find(Order.class)
        .setUseDocStore(true)
        .where()
        .conjunction().isNotNull("status").gt("customer.id", 1)
        .query();

    List<Order> list = query.findList();
    assertThat(list).hasSize(1);
  }

  @Test
  public void logicConjunction_when() {

    Query<Order> query = Ebean.find(Order.class)
        .setUseDocStore(true)
        .where()
        .and(Expr.isNotNull("status"), Expr.gt("customer.id", 1))
        .query();

    List<Order> list = query.findList();
    assertThat(list).hasSize(1);
  }

  @Test
  public void logicDisjunction_when() {

    Query<Order> query = Ebean.find(Order.class)
        .setUseDocStore(true)
        .where()
        .or(Expr.isNotNull("status"), Expr.gt("customer.id", 1))
        .query();

    List<Order> list = query.findList();
    assertThat(list).hasSize(3);
  }

  @Test
  public void existsQuery_when() {

    Query<Order> query = Ebean.find(Order.class)
        .setUseDocStore(true)
        .where()
        .or(Expr.isNotNull("status"), Expr.gt("customer.id", 1))
        .query();

    List<Order> list = query.findList();
    assertThat(list).hasSize(3);
  }


  @Test
  public void greaterThan() {

    Query<Order> query = Ebean.find(Order.class)
        .setUseDocStore(true)
        .where()
        .gt("customer.id", 1)
        .query();

    List<Order> list = query.findList();
    assertThat(list).hasSize(2);
  }

  @Test
  public void between() {

    Query<Order> query = Ebean.find(Order.class)
        .setUseDocStore(true)
        .where()
        .between("customer.id", 1, 2)
        .query();

    List<Order> list = query.findList();
    assertThat(list).hasSize(5);
  }

  @Test
  public void betweenProperty() {

    long now = System.currentTimeMillis();

    Query<Order> query = Ebean.find(Order.class)
        .setUseDocStore(true)
        .where()
        .betweenProperties("orderDate", "shipDate", now)
        .query();

    List<Order> list = query.findList();
    assertThat(list).hasSize(0);
  }

  @Test
  public void allEquals() {

    Map<String, Object> allEq = new HashMap<String,Object>();
    allEq.put("status", "COMPLETE");
    allEq.put("customer.id", 1);

    Query<Order> query = Ebean.find(Order.class)
        .setUseDocStore(true)
        .where()
        .allEq(allEq)
        .query();

    List<Order> list = query.findList();
    assertThat(list).hasSize(1);
  }


  @Test
  public void ieq() {

    Query<Order> query = Ebean.find(Order.class)
        .setUseDocStore(true)
        .where()
        .ieq("customer.name", "Rob")
        .query();

    List<Order> list = query.findList();
    assertThat(list).hasSize(3);
  }

  @Test
  public void ieq_when_hasMultipleTermsSpaces() {

    Query<Order> query = Ebean.find(Order.class)
        .setUseDocStore(true)
        .where()
        .ieq("customer.name", "Cust Noaddress")
        .query();

    List<Order> list = query.findList();
    assertThat(list).hasSize(2);
  }


  @Test
  public void jsonPathBetween_when() {

    Query<Order> query = Ebean.find(Order.class)
        .setUseDocStore(true)
        .where()
        .jsonBetween("customer", "id", 1, 2)
        .query();

    List<Order> list = query.findList();
    assertThat(list).hasSize(5);
  }

  @Test
  public void jsonPathEqualTo_when() {

    Query<Order> query = Ebean.find(Order.class)
        .setUseDocStore(true)
        .where()
        .jsonEqualTo("customer", "id", 1)
        .query();

    List<Order> list = query.findList();
    assertThat(list).hasSize(3);
  }

  @Test
  public void integration_test() {

    Query<Order> query = Ebean.find(Order.class)
        .setUseDocStore(true)
        .select("status")
        .fetch("customer","id,name")
        .fetch("details","id")
        .fetch("details.product","id")
        .where().eq("customer.id", 1)
        .query();

    List<Order> list = query.findList();
    System.out.print("as" + list);
  }


  @Test
  public void integration_test_findEach() {

    Query<Order> query = Ebean.find(Order.class)
        .setUseDocStore(true)
        .select("status")
        .fetch("customer","id,name")
        .fetch("details","id")
        .fetch("details.product","id")
        .where().eq("customer.id", 1)
        .query();

    query.findEach(new QueryEachConsumer<Order>() {
      @Override
      public void accept(Order bean) {
        System.out.print("bean" + bean);
      }
    });
  }
}