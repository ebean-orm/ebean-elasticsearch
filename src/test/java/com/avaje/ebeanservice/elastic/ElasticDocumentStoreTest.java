package com.avaje.ebeanservice.elastic;

import com.avaje.ebean.DocumentStore;
import com.avaje.ebean.Ebean;
import com.avaje.ebean.Expr;
import com.avaje.ebean.PagedList;
import com.avaje.ebean.Query;
import com.avaje.ebean.QueryEachConsumer;
import org.example.domain.Contact;
import org.example.domain.Country;
import org.example.domain.Customer;
import org.example.domain.Order;
import org.example.domain.Product;
import org.example.integrationtests.ResetBasicData;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

public class ElasticDocumentStoreTest {

  @Test
  public void indexAll() {

    ResetBasicData.reset(false);

    DocumentStore documentStore = Ebean.getDefaultServer().docStore();

    documentStore.indexAll(Country.class);
    documentStore.indexAll(Product.class);
    documentStore.indexAll(Customer.class);
  }

  @Test
  public void indexCopyTo() {

    DocumentStore documentStore = Ebean.getDefaultServer().docStore();
    //documentStore.indexAll(Product.class);

    String newIndex = "product_v2";
    documentStore.copyIndex(Product.class, newIndex);
  }

  @Test
  public void indexCopyTo_since() throws InterruptedException {

    ResetBasicData.reset(false);

    DocumentStore documentStore = Ebean.getDefaultServer().docStore();
    documentStore.indexAll(Product.class);

    String newIndex = "product_v2";
    documentStore.dropIndex(newIndex);
    documentStore.createIndex(newIndex, null, "product_v2");

    long startEpochMillis = System.currentTimeMillis();

    documentStore.copyIndex(Product.class, newIndex);

    Product prod = Ebean.find(Product.class)
        .where().eq("name", "Chair")
        .findUnique();

    prod.setSku("C00Z");
    Ebean.save(prod);

    Thread.sleep(3000);

    // copy any changes since startEpochMillis
    documentStore.copyIndex(Product.class, newIndex, startEpochMillis);
  }

  @Test
  public void sortBy_when_propertyIsAnalysed() throws InterruptedException {

    ResetBasicData.reset(false);

    DocumentStore documentStore = Ebean.getDefaultServer().docStore();
//    documentStore.indexAll(Order.class);

    //Thread.sleep(2000);
    Query<Order> query = Ebean.find(Order.class)
        .orderBy().asc("customer.name");//"orderDate.name");

    List<Order> list = documentStore.findList(query);
    System.out.print("as" + list);
  }

  @Test
  public void findPagedList() throws InterruptedException {

    ResetBasicData.reset(false);
    DocumentStore documentStore = Ebean.getDefaultServer().docStore();
    documentStore.indexAll(Order.class);

    Thread.sleep(3000);
    Query<Order> query = Ebean.find(Order.class)
        .where().in("customer.id", 1, 2)
        .setMaxRows(2)
        .orderBy().asc("customer.name");

    PagedList<Order> list = documentStore.findPagedList(query);

    assertThat(list.getTotalRowCount()).isEqualTo(5);
    assertThat(list.getList()).hasSize(2);
  }

  @Test
  public void query_useDocStore_then_lazyLoadAssocMany() throws InterruptedException {

    ResetBasicData.reset(false);

    DocumentStore documentStore = Ebean.getDefaultServer().docStore();
    documentStore.indexAll(Customer.class);
    documentStore.indexAll(Contact.class);

    Thread.sleep(3000);
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

    ResetBasicData.reset(false);

    DocumentStore documentStore = Ebean.getDefaultServer().docStore();
    documentStore.indexAll(Customer.class);
    documentStore.indexAll(Contact.class);

    Thread.sleep(3000);
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

    ResetBasicData.reset(false);

    DocumentStore documentStore = Ebean.getDefaultServer().docStore();
    documentStore.indexAll(Order.class);

    Query<Order> query = Ebean.find(Order.class)
        .where().eq("customer.name", "Rob")
        .orderBy().asc("customer.name");

    List<Order> list1 = documentStore.findList(query);

    Query<Order> query2 = Ebean.find(Order.class)
        .where().ne("customer.name", "Rob")
        .orderBy().asc("customer.name");

    List<Order> list2 = documentStore.findList(query2);

    assertThat(list1).hasSize(3);
    assertThat(list2).hasSize(2);
  }

  @Test
  public void in_when() {

//    ResetBasicData.reset(false);
//
    DocumentStore documentStore = Ebean.getDefaultServer().docStore();
//    documentStore.indexAll(Order.class);

    Query<Order> query = Ebean.find(Order.class)
        .where().in("customer.id", 1, 2)
        .orderBy().asc("customer.name");

    List<Order> list = documentStore.findList(query);

    assertThat(list).hasSize(5);
  }

  @Test
  public void idsIn_when() {

//    ResetBasicData.reset(false);
//
    DocumentStore documentStore = Ebean.getDefaultServer().docStore();
//    documentStore.indexAll(Order.class);

    List<Integer> ids = Arrays.asList(1, 3);
    Query<Order> query = Ebean.find(Order.class)
        .where().idIn(ids)
        .orderBy().asc("customer.name");

    List<Order> list = documentStore.findList(query);

    assertThat(list).hasSize(2);
  }

  @Test
  public void exists_when() {

//    ResetBasicData.reset(false);
//
    DocumentStore documentStore = Ebean.getDefaultServer().docStore();
//    documentStore.indexAll(Order.class);

    Query<Order> query = Ebean.find(Order.class)
        .where().isNotNull("status").query();

    List<Order> list = documentStore.findList(query);

    assertThat(list).hasSize(2);
  }

  @Test
  public void exists_when_multipleFields() {

//    ResetBasicData.reset(false);
//
    DocumentStore documentStore = Ebean.getDefaultServer().docStore();
//    documentStore.indexAll(Order.class);

    Query<Order> query = Ebean.find(Order.class)
        .where().isNotNull("status").isNotNull("customer.id")
        .query();

    List<Order> list = documentStore.findList(query);

    assertThat(list).hasSize(2);
  }

  @Test
  public void notExists_when() {

//    ResetBasicData.reset(false);
//
    DocumentStore documentStore = Ebean.getDefaultServer().docStore();
//    documentStore.indexAll(Order.class);

    Query<Order> query = Ebean.find(Order.class)
        .where().isNull("status").query();

    List<Order> list = documentStore.findList(query);

    assertThat(list).hasSize(3);
  }


  @Test
  public void disjunction_when() {

    DocumentStore documentStore = Ebean.getDefaultServer().docStore();
    Query<Order> query = Ebean.find(Order.class)
        .where()
        .disjunction().isNotNull("status").gt("customer.id", 1)
        .query();

    List<Order> list = documentStore.findList(query);

    assertThat(list).hasSize(2);
  }


  @Test
  public void conjunction_when() {

    DocumentStore documentStore = Ebean.getDefaultServer().docStore();
    Query<Order> query = Ebean.find(Order.class)
        .where()
        .conjunction().isNotNull("status").gt("customer.id", 1)
        .query();

    List<Order> list = documentStore.findList(query);

    assertThat(list).hasSize(1);
  }

  @Test
  public void logicConjunction_when() {

    DocumentStore documentStore = Ebean.getDefaultServer().docStore();
    Query<Order> query = Ebean.find(Order.class)
        .where()
        .and(Expr.isNotNull("status"), Expr.gt("customer.id", 1))
        .query();

    List<Order> list = documentStore.findList(query);

    assertThat(list).hasSize(1);
  }

  @Test
  public void logicDisjunction_when() {

    DocumentStore documentStore = Ebean.getDefaultServer().docStore();
    Query<Order> query = Ebean.find(Order.class)
        .where()
        .or(Expr.isNotNull("status"), Expr.gt("customer.id", 1))
        .query();

    List<Order> list = documentStore.findList(query);

    assertThat(list).hasSize(3);
  }

  @Test
  public void existsQuery_when() {

    DocumentStore documentStore = Ebean.getDefaultServer().docStore();
    Query<Order> query = Ebean.find(Order.class)
        .where()
        .or(Expr.isNotNull("status"), Expr.gt("customer.id", 1))
        .query();

    List<Order> list = documentStore.findList(query);

    assertThat(list).hasSize(3);
  }


  @Test
  public void greaterThan() {

    DocumentStore documentStore = Ebean.getDefaultServer().docStore();
    Query<Order> query = Ebean.find(Order.class)
        .where()
        .gt("customer.id", 1)
        .query();

    List<Order> list = documentStore.findList(query);

    assertThat(list).hasSize(2);
  }

  @Test
  public void between() {

    DocumentStore documentStore = Ebean.getDefaultServer().docStore();
    Query<Order> query = Ebean.find(Order.class)
        .where()
        .between("customer.id", 1, 2)
        .query();

    List<Order> list = documentStore.findList(query);

    assertThat(list).hasSize(5);
  }

  @Test
  public void betweenProperty() {

    long now = System.currentTimeMillis();
    DocumentStore documentStore = Ebean.getDefaultServer().docStore();

    Query<Order> query = Ebean.find(Order.class)
        .where()
        .betweenProperties("orderDate", "shipDate", now)
        .query();

    List<Order> list = documentStore.findList(query);

    assertThat(list).hasSize(0);
  }

  @Test
  public void allEquals() {

    DocumentStore documentStore = Ebean.getDefaultServer().docStore();

    Map<String, Object> allEq = new HashMap<String,Object>();
    allEq.put("status", "COMPLETE");
    allEq.put("customer.id", 1);

    Query<Order> query = Ebean.find(Order.class)
        .where()
        .allEq(allEq)
        .query();

    List<Order> list = documentStore.findList(query);

    assertThat(list).hasSize(1);
  }


  @Test
  public void ieq() {

    DocumentStore documentStore = Ebean.getDefaultServer().docStore();

    Query<Order> query = Ebean.find(Order.class)
        .where()
        .ieq("customer.name", "Rob")
        .query();

    List<Order> list = documentStore.findList(query);

    assertThat(list).hasSize(3);
  }

  @Test
  public void ieq_when_hasMultipleTermsSpaces() {

    DocumentStore documentStore = Ebean.getDefaultServer().docStore();

    Query<Order> query = Ebean.find(Order.class)
        .where()
        .ieq("customer.name", "Cust Noaddress")
        .query();

    List<Order> list = documentStore.findList(query);

    assertThat(list).hasSize(2);
  }


  @Test
  public void jsonPathBetween_when() {

    DocumentStore documentStore = Ebean.getDefaultServer().docStore();
    Query<Order> query = Ebean.find(Order.class)
        .where()
        .jsonBetween("customer", "id", 1, 2)
        .query();

    List<Order> list = documentStore.findList(query);

    assertThat(list).hasSize(5);
  }

  @Test
  public void jsonPathEqualTo_when() {

    DocumentStore documentStore = Ebean.getDefaultServer().docStore();
    Query<Order> query = Ebean.find(Order.class)
        .where()
        .jsonEqualTo("customer", "id", 1)
        .query();

    List<Order> list = documentStore.findList(query);

    assertThat(list).hasSize(3);
  }

  @Test
  public void integration_test() {


    ResetBasicData.reset(true);

    DocumentStore documentStore = Ebean.getDefaultServer().docStore();

    Query<Order> query = Ebean.find(Order.class)
        //.select("status, customer.id, customer.name, details.id")

        .select("status")
        .fetch("customer","id,name")
        .fetch("details","id")
        .fetch("details.product","id")
        .where().eq("customer.id", 1)
        .query();

    List<Order> list = documentStore.findList(query);

    System.out.print("as" + list);

    List<Order> list1 = query.findList();

    System.out.print("as" + list1);

  }


  @Test
  public void integration_test_findEach() {


    //ResetBasicData.reset();

    DocumentStore documentStore = Ebean.getDefaultServer().docStore();

    Query<Order> query = Ebean.find(Order.class)
        //.select("status, customer.id, customer.name, details.id")

        .select("status")
        .fetch("customer","id,name")
        .fetch("details","id")
        .fetch("details.product","id")
        .where().eq("customer.id", 1)
        .query();

    documentStore.findEach(query, new QueryEachConsumer<Order>() {
      @Override
      public void accept(Order bean) {
        System.out.print("bean" + bean);
      }
    });


  }
}