package integration.support;

import io.ebean.DocumentStore;
import io.ebean.Ebean;
import io.ebean.EbeanServer;
//import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.node.Node;
//import org.elasticsearch.node.NodeBuilder;
import org.example.domain.Contact;
import org.example.domain.Country;
import org.example.domain.Customer;
import org.example.domain.Order;
import org.example.domain.Product;
import org.example.domain.Vehicle;

/**
 */
public class EmbeddedServer {

  final boolean useExternalElastic = true;

  final boolean indexOnStart = true;

  final Node node;

  final EbeanServer server;

  final DocumentStore documentStore;

  public EmbeddedServer() {

    if (useExternalElastic) {
      this.node = null;

    } else {

//      Boot boot = new Boot();
//      boot.start();
//      Settings settings1 = Settings.builder()
//          .put("path.home", "target/elastic-data")
////          .put("number_of_shards", "1")
////          .put("number_of_replicas", "1")
////          .put("http.port", "9290")
//
//          .put("cluster.name", "EmbeddedTest")
//          .put("http.type", "http.type.default")
//          .put("transport.type", "local")
//          .put("node.name", "foo")
//          .build();
//
//      this.node = new Node(settings1);
      this.node = null;
    }
    server = Ebean.getDefaultServer();
    documentStore = server.docStore();

    init();
  }

  private void init() {

    SeedDbData.reset(false);

    if (indexOnStart) {
      documentStore.indexAll(Country.class);
      documentStore.indexAll(Product.class);
      documentStore.indexAll(Customer.class);
      documentStore.indexAll(Contact.class);
      documentStore.indexAll(Order.class);
      documentStore.indexAll(Vehicle.class);
      try {
        // allow the indexing time to store
        Thread.sleep(1000);
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    }
  }


  public Node getNode() {
    return node;
  }

  public EbeanServer getServer() {
    return server;
  }
}
