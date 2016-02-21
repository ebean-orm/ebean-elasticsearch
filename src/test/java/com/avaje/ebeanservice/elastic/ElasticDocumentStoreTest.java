package com.avaje.ebeanservice.elastic;

import com.avaje.ebean.DocumentStore;
import com.avaje.ebean.Ebean;
import com.avaje.ebean.Query;
import com.avaje.ebean.QueryEachConsumer;
import org.example.domain.Order;
import org.example.integrationtests.ResetBasicData;
import org.junit.Test;

import java.util.List;

public class ElasticDocumentStoreTest {

  @Test
  public void integration_test() {


    ResetBasicData.reset();

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