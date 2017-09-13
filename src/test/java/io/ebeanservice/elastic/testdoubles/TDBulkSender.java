package io.ebeanservice.elastic.testdoubles;

import io.ebean.config.JsonConfig;
import io.ebeanservice.elastic.bulk.BulkBuffer;
import io.ebeanservice.elastic.bulk.BulkSender;
import com.fasterxml.jackson.core.JsonFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;

public class TDBulkSender extends BulkSender {

  public BulkBuffer request;

  public Map<String, Object> response = Collections.emptyMap();

  public void resetForTesting() {
    request = null;
  }


  public TDBulkSender() {
    super(new JsonFactory(), JsonConfig.Include.NON_EMPTY, null, null);
  }

  @Override
  public Map<String, Object> sendBulk(BulkBuffer buffer) throws IOException {
    request = buffer;
    return response;
  }

}
