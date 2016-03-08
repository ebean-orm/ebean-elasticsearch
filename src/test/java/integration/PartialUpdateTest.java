package integration;

import com.avaje.ebean.Ebean;
import com.avaje.ebean.EbeanServer;
import integration.BaseTest;
import integration.support.SeedDbData;
import org.assertj.core.api.StrictAssertions;
import org.example.domain.Customer;
import org.example.domain.Product;
import org.testng.annotations.Test;

import static org.assertj.core.api.Assertions.assertThat;

@Test
public class PartialUpdateTest extends BaseTest {

  public void partial_product_update() throws InterruptedException {

    Product prod = new Product();
    prod.setSku("KB9F");
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

    sleepToPropagate();

    Product partialWithVersion = Ebean.find(Product.class)
        .select("name,version")
        .where().idEq(prod.getId())
        .findUnique();

    partialWithVersion.setName("Silly Keyboard");
    Ebean.save(partialWithVersion);

    assertThat(Ebean.getBeanState(partialWithVersion).getLoadedProps()).contains("whenModified", "version");

  }

  public void partial_update() throws InterruptedException {

    Customer rob = server.find(Customer.class)
        .select("id, status, name, smallNote")
        .where().eq("name", "Rob")
        .findUnique();

    rob.setSmallNote("Modify small note");
    server.save(rob);

    sleepToPropagate();

    Customer robDoc = server.docStore().find(Customer.class, rob.getId());

    assertThat(robDoc.getSmallNote()).isEqualTo(rob.getSmallNote());
    assertThat(robDoc.getStatus()).isEqualTo(rob.getStatus());
    assertThat(robDoc.getName()).isEqualTo("Rob");
    assertThat(robDoc.getBillingAddress().getCountry().getCode()).isEqualTo("NZ");

  }
}
