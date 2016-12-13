package integration;

import io.ebean.DocumentStore;
import io.ebean.EbeanServer;
import integration.support.EmbeddedServer;

public abstract class BaseTest {

  public static final EmbeddedServer embeddedServer = new EmbeddedServer();

  public static EbeanServer server = embeddedServer.getServer();

  public static DocumentStore docStore = server.docStore();

  public void sleepToPropagate() {
    try {
      Thread.sleep(1100);
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }
}
