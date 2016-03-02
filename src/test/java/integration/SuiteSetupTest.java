package integration;

import com.avaje.ebean.DocumentStore;
import com.avaje.ebean.Ebean;
import com.avaje.ebean.EbeanServer;

import integration.support.EmbeddedServer;
import integration.support.SeedDbData;
import org.example.domain.Contact;
import org.example.domain.Country;
import org.example.domain.Customer;
import org.example.domain.Order;
import org.example.domain.Product;
import org.testng.annotations.AfterGroups;
import org.testng.annotations.BeforeGroups;
import org.testng.annotations.Test;


public class SuiteSetupTest {

//  protected EmbeddedServer elastic;
//
//  protected EbeanServer server;
//
//  protected DocumentStore documentStore;
//
//  @BeforeGroups("store")
//  public void setup() {
//    elastic = new EmbeddedServer();
//    server = Ebean.getDefaultServer();
//    documentStore = server.docStore();
//    SeedDbData.reset(false);
//
//    documentStore.indexAll(Country.class);
//    documentStore.indexAll(Product.class);
//    documentStore.indexAll(Customer.class);
//    documentStore.indexAll(Contact.class);
//    documentStore.indexAll(Order.class);
//  }
//
//  @AfterGroups("store")
//  public void shutdown() {
//    elastic.getNode().close();
//  }
}
