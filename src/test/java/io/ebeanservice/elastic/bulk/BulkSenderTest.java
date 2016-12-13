package io.ebeanservice.elastic.bulk;

import io.ebean.config.JsonConfig;
import io.ebeanservice.elastic.testdoubles.TDIndexMessageSender;
import com.fasterxml.jackson.core.JsonFactory;
import org.jetbrains.annotations.NotNull;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;


public class BulkSenderTest {

  TDIndexMessageSender messageSender = new TDIndexMessageSender();

  @Test
  public void newBuffer() throws Exception {
    assertNotNull(createBulkSender().newBuffer());
  }

  @Test
  public void sendBulk_when_empty() throws Exception {

    BulkSender bulkSender = createBulkSender();
    BulkBuffer emptyBuffer = bulkSender.newBuffer();
    bulkSender.sendBulk(emptyBuffer);

    assertNull(messageSender.request);
  }

  @Test
  public void sendBulk_when_some() throws Exception {

    BulkSender bulkSender = createBulkSender();

    BulkBuffer buffer = bulkSender.newBuffer();
    buffer.gen().writeStartObject();
    buffer.gen().writeStringField("name","rob");
    buffer.gen().writeEndObject();

    bulkSender.sendBulk(buffer);

    assertEquals(messageSender.request, "{\"name\":\"rob\"}");
  }

  @NotNull
  private BulkSender createBulkSender() {

    messageSender.request = null;

    JsonFactory jsonFactory = new JsonFactory();
    JsonConfig.Include defaultInclude = JsonConfig.Include.NON_EMPTY;
    return new BulkSender(jsonFactory, defaultInclude, null, messageSender);
  }


}