package integration.support;

import com.avaje.ebean.Ebean;
import com.avaje.ebean.EbeanServer;
import com.avaje.ebean.Transaction;
import com.avaje.ebean.annotation.DocStoreMode;
import org.example.domain.Address;
import org.example.domain.Contact;
import org.example.domain.Country;
import org.example.domain.Customer;
import org.example.domain.Order;
import org.example.domain.OrderDetail;
import org.example.domain.OrderShipment;
import org.example.domain.Product;

import java.sql.Date;
import java.util.ArrayList;
import java.util.List;

public class SeedDbData {

  private static boolean runOnce;

  private static EbeanServer server = Ebean.getServer(null);

  public static synchronized void reset() {
    reset(true);
  }

  public static synchronized void reset(boolean updateDocStore) {

    if (runOnce) {
      return;
    }

    final SeedDbData me = new SeedDbData();

    if (server.find(Product.class).findRowCount() > 0) {
      // we can't really delete this base data as
      // the test rely on the products being in there
      return;
    }
    //me.deleteAll();

    Transaction transaction = server.beginTransaction();
    if (!updateDocStore) {
      transaction.setDocStoreMode(DocStoreMode.IGNORE);
    }
    try {

      me.insertCountries();
      me.insertProducts();
      me.insertTestCustAndOrders();
      runOnce = true;
      server.commitTransaction();
    } finally {
      server.endTransaction();
    }
  }

  public void insertCountries() {

    if (server.find(Country.class).findRowCount() > 0) {
      return;
    }

    Country c = new Country();
    c.setCode("NZ");
    c.setName("New Zealand");
    server.save(c);

    Country au = new Country();
    au.setCode("AU");
    au.setName("Australia");
    server.save(au);
  }


  public void insertProducts() {

    if (server.find(Product.class).findRowCount() > 0) {
      return;
    }

    Product p = new Product();
    p.setName("Chair");
    p.setSku("C001");
    server.save(p);

    p = new Product();
    p.setName("Desk");
    p.setSku("DSK1");
    server.save(p);

    p = new Product();
    p.setName("Computer");
    p.setSku("C002");
    server.save(p);

    p = new Product();
    p.setName("Printer");
    p.setSku("C003");
    server.save(p);

    p = new Product();
    p.setName("ZChair");
    p.setSku("Z99Z");
    server.save(p);
  }

  public void insertTestCustAndOrders() {


    Customer cust1 = insertCustomer("Rob");
    cust1.addContact(new Contact("Jim", "Cricket"));
    cust1.addContact(new Contact("Barny", "Rubble"));
    cust1.addContact(new Contact("Bugs", "Bunny"));
    Ebean.save(cust1);

    Customer cust2 = insertCustomerNoAddress("Cust NoAddress");
    insertCustomerFiona("Fiona");
    insertCustomerNoContacts("NoContactsCust");

    createOrder1(cust1);
    createOrder2(cust2);
    createOrder3(cust1);
    createOrder4(cust1);
    createOrder5(cust2);
  }

  private static int contactEmailNum = 1;

  private Customer insertCustomerFiona(String name) {

    Customer c = createCustomer(name, "12 Apple St", "West Coast Rd", "2009-08-31");
    c.setStatus(Customer.Status.ACTIVE);

    c.addContact(createContact("Fiona", "Black"));
    c.addContact(createContact("Tracy", "Red"));

    Ebean.save(c);
    return c;
  }

  public static Contact createContact(String firstName, String lastName) {
    Contact contact = new Contact(firstName, lastName);
    String email = contact.getLastName() + (contactEmailNum++) + "@test.com";
    contact.setEmail(email.toLowerCase());
    return contact;
  }

  private Customer insertCustomerNoContacts(String name) {

    Customer c = createCustomer("Roger", "15 Kumera Way", "Bos town", "2010-04-10");
    c.setName(name);
    c.setStatus(Customer.Status.ACTIVE);

    Ebean.save(c);
    return c;
  }

  private Customer insertCustomerNoAddress(String name) {

    Customer c = new Customer();
    c.setName(name);
    c.setStatus(Customer.Status.NEW);
    c.addContact(createContact("Jack", "Black"));

    Ebean.save(c);
    return c;
  }

  private static Customer insertCustomer(String name) {
    return createCustomer(name, "1 Banana St", "P.O.Box 1234",  null);
  }

  public static Customer createCustomer(String name, String shippingStreet, String billingStreet, String annDate) {

    Customer c = new Customer();
    c.setName(name);
    c.setStatus(Customer.Status.NEW);
    if (annDate == null) {
      annDate = "2010-04-14";
    }
    c.setAnniversary(Date.valueOf(annDate));

    if (shippingStreet != null) {
      Address shippingAddr = new Address();
      shippingAddr.setLine1(shippingStreet);
      shippingAddr.setLine2("Sandringham");
      shippingAddr.setCity("Auckland");
      shippingAddr.setCountry(Ebean.getReference(Country.class, "NZ"));

      c.setShippingAddress(shippingAddr);
    }

    if (billingStreet != null) {
      Address billingAddr = new Address();
      billingAddr.setLine1(billingStreet);
      billingAddr.setLine2("St Lukes");
      billingAddr.setCity("Auckland");
      billingAddr.setCountry(Ebean.getReference(Country.class, "NZ"));

      c.setBillingAddress(billingAddr);
    }

    return c;
  }

  private Order createOrder1(Customer customer) {

    Product product1 = Ebean.getReference(Product.class, 1);
    Product product2 = Ebean.getReference(Product.class, 2);
    Product product3 = Ebean.getReference(Product.class, 3);


    Order order = new Order();
    order.setCustomer(customer);

    List<OrderDetail> details = new ArrayList<OrderDetail>();
    details.add(new OrderDetail(product1, 5, 10.50));
    details.add(new OrderDetail(product2, 3, 1.10));
    details.add(new OrderDetail(product3, 1, 2.00));
    order.setDetails(details);


    order.getShipments().add(new OrderShipment());

    Ebean.save(order);
    return order;
  }

  private void createOrder2(Customer customer) {

    Product product1 = Ebean.getReference(Product.class, 1);

    Order order = new Order();
    order.setStatus(Order.Status.SHIPPED);
    order.setCustomer(customer);

    List<OrderDetail> details = new ArrayList<OrderDetail>();
    details.add(new OrderDetail(product1, 4, 10.50));
    order.setDetails(details);

    order.getShipments().add(new OrderShipment());

    Ebean.save(order);
  }

  private void createOrder3(Customer customer) {

    Product product1 = Ebean.getReference(Product.class, 1);
    Product product3 = Ebean.getReference(Product.class, 3);

    Order order = new Order();
    order.setStatus(Order.Status.COMPLETE);
    order.setCustomer(customer);

    List<OrderDetail> details = new ArrayList<OrderDetail>();
    details.add(new OrderDetail(product1, 3, 10.50));
    details.add(new OrderDetail(product3, 40, 2.10));
    details.add(new OrderDetail(product1, 5, 10.00));
    order.setDetails(details);

    order.getShipments().add(new OrderShipment());

    Ebean.save(order);
  }

  private void createOrder4(Customer customer) {

    Order order = new Order();
    order.setCustomer(customer);

    order.getShipments().add(new OrderShipment());

    Ebean.save(order);
  }

  private void createOrder5(Customer customer) {

    Order order = new Order();
    order.setCustomer(customer);
    order.getShipments().add(new OrderShipment());

    Ebean.save(order);
  }
}
