package io.ebeanservice.elastic;

import io.ebean.DocStoreQueueEntry;
import integration.BaseTest;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;


public class ElasticUpdateProcessorTest extends BaseTest {

  @Test
  public void processNested() throws Exception {

    List<DocStoreQueueEntry> list = new ArrayList<DocStoreQueueEntry>();
    list.add(new DocStoreQueueEntry(DocStoreQueueEntry.Action.NESTED, "order", "customer.id", 2));

    long count = docStore.process(list);
    // 2 orders for customer 2
    assertThat(count).isEqualTo(2);
  }

  @Test
  public void processNested_when_doubleDepth() throws Exception {

    //SeedDbData.reset(false);

    List<DocStoreQueueEntry> list = new ArrayList<DocStoreQueueEntry>();
    list.add(new DocStoreQueueEntry(DocStoreQueueEntry.Action.NESTED, "order", "customer.billingAddress.id", 1));

    long count = docStore.process(list);
    // 3 orders for customer 1
    assertThat(count).isEqualTo(3);
  }

  @Test
  public void processNested_when_many() throws Exception {

    List<DocStoreQueueEntry> list = new ArrayList<DocStoreQueueEntry>();
    list.add(new DocStoreQueueEntry(DocStoreQueueEntry.Action.NESTED, "order", "details.id", 3));

    long count = docStore.process(list);
    assertThat(count).isEqualTo(1);
  }

  @Test
  public void processNested_when_manyDoubleDepth() throws Exception {

    List<DocStoreQueueEntry> list = new ArrayList<DocStoreQueueEntry>();
    list.add(new DocStoreQueueEntry(DocStoreQueueEntry.Action.NESTED, "order", "details.product.id", 2));

    long count = docStore.process(list);
    assertThat(count).isEqualTo(1);
  }

  @Test
  public void processNested_when_manyDoubleDepth2() throws Exception {

    List<DocStoreQueueEntry> list = new ArrayList<DocStoreQueueEntry>();
    list.add(new DocStoreQueueEntry(DocStoreQueueEntry.Action.NESTED, "order", "details.product.id", 1));

    long count = docStore.process(list);
    assertThat(count).isEqualTo(3);
  }

}