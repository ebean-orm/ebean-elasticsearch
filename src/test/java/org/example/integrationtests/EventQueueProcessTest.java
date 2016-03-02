package org.example.integrationtests;

import com.avaje.ebean.DocStoreQueueEntry;
import com.avaje.ebean.DocumentStore;
import com.avaje.ebean.Ebean;
import com.avaje.ebean.EbeanServer;
import integration.support.SeedDbData;
import org.example.domain.Customer;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

@Test(enabled = false)
public class EventQueueProcessTest {

  public void test_queue() throws IOException {

    SeedDbData.reset(true);

    EbeanServer server = Ebean.getDefaultServer();
    DocumentStore documentStore = server.docStore();

    List<DocStoreQueueEntry> entries = new ArrayList<DocStoreQueueEntry>();
    entries.add(new DocStoreQueueEntry(DocStoreQueueEntry.Action.INDEX, "product", 1));
    entries.add(new DocStoreQueueEntry(DocStoreQueueEntry.Action.INDEX, "product", 2));

    documentStore.process(entries);

  }

  public void test_queue_nested() throws IOException {

    SeedDbData.reset();

    EbeanServer server = Ebean.getDefaultServer();
    DocumentStore documentStore = server.docStore();

    List<DocStoreQueueEntry> entries = new ArrayList<DocStoreQueueEntry>();
    entries.add(new DocStoreQueueEntry(DocStoreQueueEntry.Action.NESTED, "order", "customer", 1));
    entries.add(new DocStoreQueueEntry(DocStoreQueueEntry.Action.NESTED, "order", "customer", 2));

    documentStore.process(entries);
  }

  public void test_embedded() throws InterruptedException {

    SeedDbData.reset();


    Customer customer = Ebean.find(Customer.class, 1);
    customer.setName("RobMod");
    Ebean.save(customer);

    Thread.sleep(10000);
  }

}
