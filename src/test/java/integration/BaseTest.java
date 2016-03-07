package integration;

import com.avaje.ebean.DocumentStore;
import com.avaje.ebean.EbeanServer;
import integration.support.EmbeddedServer;

public abstract class BaseTest {

  public static final EmbeddedServer embeddedServer = new EmbeddedServer();

  public static EbeanServer server = embeddedServer.getServer();

  public static DocumentStore docStore = server.docStore();

  public void sleepToPropagate(long millis) {
    try {
      Thread.sleep(millis);
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }
}
