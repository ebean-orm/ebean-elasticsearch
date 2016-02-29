package com.avaje.ebeanservice.elastic;

import com.avaje.ebean.DocStoreQueueEntry;
import com.avaje.ebean.Ebean;
import com.avaje.ebeaninternal.api.SpiEbeanServer;
import com.avaje.ebeaninternal.server.deploy.BeanDescriptor;
import com.avaje.ebeanservice.docstore.api.DocStoreUpdates;
import com.avaje.ebeanservice.docstore.api.support.DocStoreDeleteEvent;
import com.avaje.ebeanservice.elastic.support.BaseHttpMessageSender;
import com.avaje.ebeanservice.elastic.support.IndexMessageSender;
import com.avaje.ebeanservice.elastic.support.IndexQueueWriter;
import com.fasterxml.jackson.core.JsonFactory;
import org.example.domain.Contact;
import org.example.integrationtests.ResetBasicData;
import org.junit.Ignore;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;


public class ElasticUpdateProcessorTest {

  SpiEbeanServer server = (SpiEbeanServer)Ebean.getServer(null);

  TDIndexQueueWriter indexQueue = new TDIndexQueueWriter();

  JsonFactory jsonFactory = new JsonFactory();

  ElasticUpdateProcessor processor = create();

  BeanDescriptor<Contact> contactBeanDescriptor = server.getBeanDescriptor(Contact.class);

  private ElasticUpdateProcessor create() {

    IndexMessageSender messageSender = new BaseHttpMessageSender("http://localhost:9200/_bulk");
    return new ElasticUpdateProcessor(indexQueue, jsonFactory, null, messageSender, 1000);
  }

  //@Ignore
  @Test
  public void testProcessNested() throws Exception {

    ResetBasicData.reset(false);

    List<DocStoreQueueEntry> list = new ArrayList<DocStoreQueueEntry>();
    list.add(new DocStoreQueueEntry(DocStoreQueueEntry.Action.NESTED, "order","customer.id", 2));

    server.docStore().process(list);
    assertEquals(2, indexQueue.theQueue.size());
  }

  @Test
  public void testProcessDoubleNested() throws Exception {

    ResetBasicData.reset(false);

    List<DocStoreQueueEntry> list = new ArrayList<DocStoreQueueEntry>();
    list.add(new DocStoreQueueEntry(DocStoreQueueEntry.Action.NESTED, "order","customer.billingAddress.id", 1));

    server.docStore().process(list);
    assertEquals(2, indexQueue.theQueue.size());
  }

  @Test
  public void testProcessNested_when_many() throws Exception {

    ResetBasicData.reset(false);

    List<DocStoreQueueEntry> list = new ArrayList<DocStoreQueueEntry>();
    list.add(new DocStoreQueueEntry(DocStoreQueueEntry.Action.NESTED, "order","details.id", 3));

    long count = server.docStore().process(list);
    assertEquals(count, 1);
  }

  @Ignore
  @Test
  public void testProcess() throws Exception {

    DocStoreDeleteEvent bulkReq0 = createDeleteContactById(1300);
    DocStoreDeleteEvent bulkReq1 = createDeleteContactById(1301);
    DocStoreDeleteEvent bulkReq2 = createDeleteContactById(1302);

    DocStoreUpdates updates = new DocStoreUpdates();
    updates.queueIndex("contact", 1);
    updates.queueIndex("contact", 2);
    updates.addDelete(bulkReq0);
    updates.addDelete(bulkReq1);
    updates.addDelete(bulkReq2);

    processor.process(updates, 1000);

    assertEquals(2, indexQueue.theQueue.size());
  }

  @Test
  public void testCreateBulkElasticUpdate() throws Exception {

    assertNotNull(processor.createBulkElasticUpdate());
  }

  @Test
  public void testCreateBatches() {


    List<DocStoreDeleteEvent> allRequests = new ArrayList<DocStoreDeleteEvent>();

    for (int i = 0; i < 33; i++) {
      allRequests.add(createDeleteContactById(i));
    }
    List<List<DocStoreDeleteEvent>> batches = processor.createBatches(allRequests, 10);

    assertEquals(4, batches.size());
    assertEquals(10, batches.get(0).size());
    assertEquals(10, batches.get(1).size());
    assertEquals(10, batches.get(2).size());
    assertEquals(3, batches.get(3).size());
  }

  @Test
  public void testCreateBatchesSingle() {

    List<DocStoreDeleteEvent> allRequests = new ArrayList<DocStoreDeleteEvent>();

    for (int i = 0; i < 33; i++) {
      allRequests.add(createDeleteContactById(i));
    }
    List<List<DocStoreDeleteEvent>> batches = processor.createBatches(allRequests, 100);

    assertEquals(1, batches.size());
    assertEquals(33, batches.get(0).size());
  }

  @Test
  public void testCreateBatchesBoundary() {

    List<DocStoreDeleteEvent> allRequests = new ArrayList<DocStoreDeleteEvent>();

    for (int i = 0; i < 33; i++) {
      allRequests.add(createDeleteContactById(i));
    }
    List<List<DocStoreDeleteEvent>> batches = processor.createBatches(allRequests, 11);

    assertEquals(3, batches.size());
    assertEquals(11, batches.get(0).size());
    assertEquals(11, batches.get(1).size());
    assertEquals(11, batches.get(2).size());
  }

  @Test
  public void testCreateBatchesUnderBoundary() {

    List<DocStoreDeleteEvent> allRequests = new ArrayList<DocStoreDeleteEvent>();

    for (int i = 0; i < 29; i++) {
      allRequests.add(createDeleteContactById(i));
    }
    List<List<DocStoreDeleteEvent>> batches = processor.createBatches(allRequests, 10);

    assertEquals(3, batches.size());
    assertEquals(10, batches.get(0).size());
    assertEquals(10, batches.get(1).size());
    assertEquals(9, batches.get(2).size());
  }

  private DocStoreDeleteEvent createDeleteContactById(long id) {
    return new DocStoreDeleteEvent(contactBeanDescriptor, id);
  }

  class TDIndexQueueWriter implements IndexQueueWriter {

    List<DocStoreQueueEntry> theQueue = new ArrayList<DocStoreQueueEntry>();

    @Override
    public void queue(List<DocStoreQueueEntry> queueEntries) {

      theQueue.addAll(queueEntries);
    }
  }
}