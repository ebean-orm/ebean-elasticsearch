package integration;

import org.example.domain.Contact;
import org.example.domain.Customer;
import org.example.domain.Order;
import org.testng.annotations.Test;

import java.util.List;

import static org.testng.Assert.assertEquals;

public class SyncCustNameUpdateTest extends BaseTest {

  @Test
  public void update() throws InterruptedException {

    Customer cust = server.find(Customer.class)
        .where().idEq(2)
        .setUseDocStore(true)
        .findOne();

    cust.setName("Cust NonAddress");
    cust.save();

    sleepToPropagate();

    //Thread.sleep(4000);

    List<Order> orders = server.find(Order.class)
        .where().eq("customer.id", cust.getId())
        .setUseDocStore(true)
        .findList();

    Thread.sleep(2000);

    for (Order order : orders) {
      assertEquals(order.getCustomer().getName(), cust.getName());
    }

    List<Contact> contacts = server.find(Contact.class)
        .where().eq("customer.id", cust.getId())
        .setUseDocStore(true)
        .findList();

    for (Contact contact : contacts) {
      assertEquals(contact.getCustomer().getName(), cust.getName());
    }
  }
}
