package com.avaje.ebeanservice.elastic;

import com.avaje.ebean.DocStoreQueueEntry;
import com.avaje.ebean.Ebean;
import com.avaje.ebean.QueryEachConsumer;
import com.avaje.ebean.plugin.BeanType;
import com.avaje.ebean.plugin.SpiServer;
import com.avaje.ebeanservice.docstore.api.support.DocStoreDeleteEvent;
import integration.BaseTest;
import integration.support.SeedDbData;
import org.example.domain.Contact;
import org.example.domain.Order;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.StrictAssertions.assertThat;


public class ElasticUpdateProcessorTest extends BaseTest {

  SpiServer server = Ebean.getServer(null).getPluginApi();

//  TDIndexQueueWriter indexQueue = new TDIndexQueueWriter();
//
//  JsonFactory jsonFactory = new JsonFactory();
//
//  ElasticUpdateProcessor processor = create();

  BeanType<Contact> contactBeanDescriptor = server.getBeanType(Contact.class);

//  private ElasticUpdateProcessor create() {
//
//    IndexMessageSender messageSender = new BaseHttpMessageSender("http://localhost:9200/_bulk");
//    return new ElasticUpdateProcessor(server, indexQueue, jsonFactory, null, messageSender, 1000);
//  }

  //@Ignore
  @Test
  public void processNested() throws Exception {

    List<DocStoreQueueEntry> list = new ArrayList<DocStoreQueueEntry>();
    list.add(new DocStoreQueueEntry(DocStoreQueueEntry.Action.NESTED, "order", "customer.id", 2));

    long count = server.docStore().process(list);
    // 2 orders for customer 2
    assertThat(count).isEqualTo(2);
  }

  @Test
  public void processNested_when_doubleDepth() throws Exception {

    //SeedDbData.reset(false);

    List<DocStoreQueueEntry> list = new ArrayList<DocStoreQueueEntry>();
    list.add(new DocStoreQueueEntry(DocStoreQueueEntry.Action.NESTED, "order", "customer.billingAddress.id", 1));

    long count = server.docStore().process(list);
    // 3 orders for customer 1
    assertThat(count).isEqualTo(3);
  }

  @Test
  public void processNested_when_many() throws Exception {

    List<DocStoreQueueEntry> list = new ArrayList<DocStoreQueueEntry>();
    list.add(new DocStoreQueueEntry(DocStoreQueueEntry.Action.NESTED, "order", "details.id", 3));

    long count = server.docStore().process(list);
    assertThat(count).isEqualTo(1);
  }

  @Test
  public void processNested_when_manyDoubleDepth() throws Exception {

    List<DocStoreQueueEntry> list = new ArrayList<DocStoreQueueEntry>();
    list.add(new DocStoreQueueEntry(DocStoreQueueEntry.Action.NESTED, "order", "details.product.id", 2));

    long count = server.docStore().process(list);
    assertThat(count).isEqualTo(1);
  }

  @Test
  public void processNested_when_manyDoubleDepth2() throws Exception {

    List<DocStoreQueueEntry> list = new ArrayList<DocStoreQueueEntry>();
    list.add(new DocStoreQueueEntry(DocStoreQueueEntry.Action.NESTED, "order", "details.product.id", 1));

    long count = server.docStore().process(list);
    assertThat(count).isEqualTo(3);
  }

//
//  @Test(enabled = false)
//  public void testProcess() throws Exception {
//
//    DocStoreDeleteEvent bulkReq0 = createDeleteContactById(1300);
//    DocStoreDeleteEvent bulkReq1 = createDeleteContactById(1301);
//    DocStoreDeleteEvent bulkReq2 = createDeleteContactById(1302);
//
//    DocStoreUpdates updates = new DocStoreUpdates();
//    updates.queueIndex("contact", 1);
//    updates.queueIndex("contact", 2);
//    updates.addDelete(bulkReq0);
//    updates.addDelete(bulkReq1);
//    updates.addDelete(bulkReq2);
//
//    server.docStore().process(updates.get);
//
//    assertEquals(2, indexQueue.theQueue.size());
//  }

//  @Test
//  public void testCreateBulkElasticUpdate() throws Exception {
//
//    assertNotNull(processor.createBulkBuffer());
//  }

  //  @Test
//  public void testCreateBatches() {
//
//
//    List<DocStoreDeleteEvent> allRequests = new ArrayList<DocStoreDeleteEvent>();
//
//    for (int i = 0; i < 33; i++) {
//      allRequests.add(createDeleteContactById(i));
//    }
//    List<List<DocStoreDeleteEvent>> batches = processor.createBatches(allRequests, 10);
//
//    assertEquals(4, batches.size());
//    assertEquals(10, batches.get(0).size());
//    assertEquals(10, batches.get(1).size());
//    assertEquals(10, batches.get(2).size());
//    assertEquals(3, batches.get(3).size());
//  }
//
//  @Test
//  public void testCreateBatchesSingle() {
//
//    List<DocStoreDeleteEvent> allRequests = new ArrayList<DocStoreDeleteEvent>();
//
//    for (int i = 0; i < 33; i++) {
//      allRequests.add(createDeleteContactById(i));
//    }
//    List<List<DocStoreDeleteEvent>> batches = processor.createBatches(allRequests, 100);
//
//    assertEquals(1, batches.size());
//    assertEquals(33, batches.get(0).size());
//  }
//
//  @Test
//  public void testCreateBatchesBoundary() {
//
//    List<DocStoreDeleteEvent> allRequests = new ArrayList<DocStoreDeleteEvent>();
//
//    for (int i = 0; i < 33; i++) {
//      allRequests.add(createDeleteContactById(i));
//    }
//    List<List<DocStoreDeleteEvent>> batches = processor.createBatches(allRequests, 11);
//
//    assertEquals(3, batches.size());
//    assertEquals(11, batches.get(0).size());
//    assertEquals(11, batches.get(1).size());
//    assertEquals(11, batches.get(2).size());
//  }
//
//  @Test
//  public void testCreateBatchesUnderBoundary() {
//
//    List<DocStoreDeleteEvent> allRequests = new ArrayList<DocStoreDeleteEvent>();
//
//    for (int i = 0; i < 29; i++) {
//      allRequests.add(createDeleteContactById(i));
//    }
//    List<List<DocStoreDeleteEvent>> batches = processor.createBatches(allRequests, 10);
//
//    assertEquals(3, batches.size());
//    assertEquals(10, batches.get(0).size());
//    assertEquals(10, batches.get(1).size());
//    assertEquals(9, batches.get(2).size());
//  }
//
  private DocStoreDeleteEvent createDeleteContactById(long id) {
    return new DocStoreDeleteEvent(contactBeanDescriptor, id);
  }
//
//  class TDIndexQueueWriter implements IndexQueueWriter {
//
//    List<DocStoreQueueEntry> theQueue = new ArrayList<DocStoreQueueEntry>();
//
//    @Override
//    public void queue(List<DocStoreQueueEntry> queueEntries) {
//      theQueue.addAll(queueEntries);
//    }
//
//    @Override
//    public void onStartup() {
//    }
//  }
}