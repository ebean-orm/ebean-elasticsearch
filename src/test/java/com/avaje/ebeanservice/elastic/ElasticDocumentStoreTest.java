package com.avaje.ebeanservice.elastic;

import com.avaje.ebean.DocumentStore;
import com.avaje.ebean.Ebean;
import com.avaje.ebean.Query;
import com.avaje.ebean.QueryEachConsumer;
import org.example.domain.Country;
import org.example.domain.Customer;
import org.example.domain.Order;
import org.example.domain.Product;
import org.example.integrationtests.ResetBasicData;
import org.junit.Test;

import java.util.List;

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
  public void sortBy_when_propertyIsAnalysed() {

    ResetBasicData.reset(false);

    DocumentStore documentStore = Ebean.getDefaultServer().docStore();
    documentStore.indexAll(Order.class);

    Query<Order> query = Ebean.find(Order.class)
        .orderBy().asc("customer.name");//"orderDate.name");

    List<Order> list = documentStore.findList(query);
    System.out.print("as" + list);
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