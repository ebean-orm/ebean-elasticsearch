package org.example.integrationtests;

import com.avaje.ebean.Ebean;
import com.avaje.ebean.EbeanServer;
import org.assertj.core.api.StrictAssertions;
import org.example.domain.Customer;
import org.example.domain.Product;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 */
public class PartialUpdateTest {

  @Test
  public void partial_product_update() throws InterruptedException {

    Product prod = new Product();
    prod.setSku("KB91");
    prod.setName("Night Keyboard");

    Ebean.save(prod);

    Thread.sleep(100);

    Product partial = Ebean.find(Product.class)
        .select("name")
        .where().idEq(prod.getId())
        .findUnique();

    partial.setName("Mighty Keyboard");
    Ebean.save(partial);

    assertThat(Ebean.getBeanState(partial).getLoadedProps()).contains("whenModified");
    assertThat(Ebean.getBeanState(partial).getLoadedProps()).doesNotContain("version");

    Thread.sleep(2000);

    Product partialWithVersion = Ebean.find(Product.class)
        .select("name,version")
        .where().idEq(prod.getId())
        .findUnique();

    partialWithVersion.setName("Silly Keyboard");
    Ebean.save(partialWithVersion);

    assertThat(Ebean.getBeanState(partialWithVersion).getLoadedProps()).contains("whenModified", "version");

    Thread.sleep(2000);

  }

  @Test
  public void partial_update() throws InterruptedException {

    // this will automatically index all the customers
    ResetBasicData.reset();

    Thread.sleep(200);

    EbeanServer server = Ebean.getDefaultServer();

    Customer rob = server.find(Customer.class)
        .select("id, status, name, smallNote")
        .where().eq("name", "Rob")
        .findUnique();

    //rob.setName("Rob B");
    rob.setSmallNote("Modify small note");
    server.save(rob);

    // wait for the update to propagate to Elastic
    Thread.sleep(1200);

    Customer robDoc = server.docStore().getById(Customer.class, rob.getId());

    StrictAssertions.assertThat(robDoc.getSmallNote()).isEqualTo(rob.getSmallNote());
    assertThat(robDoc.getStatus()).isEqualTo(rob.getStatus());
    StrictAssertions.assertThat(robDoc.getName()).isEqualTo("Rob");
    StrictAssertions.assertThat(robDoc.getBillingAddress().getCountry().getCode()).isEqualTo("NZ");


  }
}
