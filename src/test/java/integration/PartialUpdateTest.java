package integration;

import io.ebean.DB;
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

    DB.save(prod);

    Thread.sleep(100);

    Product partial = DB.find(Product.class)
        .select("name")
        .where().idEq(prod.getId())
        .findOne();

    partial.setName("Mighty Keyboard");
    DB.save(partial);

    assertThat(DB.beanState(partial).loadedProps()).contains("whenModified");
    assertThat(DB.beanState(partial).loadedProps()).doesNotContain("version");

    sleepToPropagate();

    Product partialWithVersion = DB.find(Product.class)
        .select("name,version")
        .where().idEq(prod.getId())
        .findOne();

    partialWithVersion.setName("Silly Keyboard");
    DB.save(partialWithVersion);

    assertThat(DB.beanState(partialWithVersion).loadedProps()).contains("whenModified", "version");

  }

  public void partial_update() {

    Customer rob = server.find(Customer.class)
        .select("id, status, name, smallNote")
        .where().eq("name", "Rob")
        .findOne();

    rob.setSmallNote("Modify small note");
    server.save(rob);

    sleepToPropagate();

    Customer robDoc = server.find(Customer.class)
        .setId(rob.getId()).setUseDocStore(true)
        .findOne();

    assertThat(robDoc.getSmallNote()).isEqualTo(rob.getSmallNote());
    assertThat(robDoc.getStatus()).isEqualTo(rob.getStatus());
    assertThat(robDoc.getName()).isEqualTo("Rob");
    assertThat(robDoc.getBillingAddress().getCountry().getCode()).isEqualTo("NZ");
  }
}
