package integration;

import integration.support.EmbeddedServer;
import io.ebean.Database;
import io.ebean.DocumentStore;

public abstract class BaseTest {

  public static final EmbeddedServer embeddedServer = new EmbeddedServer();

  public static Database server = embeddedServer.getServer();

  public static DocumentStore docStore = server.docStore();

  public static void sleepToPropagate() {
    try {
      Thread.sleep(1100);
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }
}
