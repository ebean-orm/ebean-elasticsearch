package integration.support;

import com.avaje.ebean.DocumentStore;
import com.avaje.ebean.Ebean;
import com.avaje.ebean.EbeanServer;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;
import org.example.domain.Contact;
import org.example.domain.Country;
import org.example.domain.Customer;
import org.example.domain.Order;
import org.example.domain.Product;

/**
 */
public class EmbeddedServer {

  final boolean useExternalElastic = true;
  final boolean indexOnStart = false;

  final Node node;

  final EbeanServer server;

  final DocumentStore documentStore;

  public EmbeddedServer() {

    if (useExternalElastic) {
      this.node = null;

    } else {
      Settings settings1 = Settings.settingsBuilder()
          .put("path.home", "target/elastic-data")
          .put("number_of_shards", "1")
          .put("number_of_replicas", "1")
          .put("cluster.name", "EmbeddedTest")
          .put("node.name", "foo")
          .build();

      this.node = NodeBuilder
          .nodeBuilder()
          .settings(settings1)
          .node();
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

//  public void stop() {
//    node.close();
//  }

}
