package io.ebeanservice.elastic;

import io.ebean.DB;
import io.ebeaninternal.api.SpiEbeanServer;
import io.ebeaninternal.server.deploy.BeanDescriptor;
import io.ebeanservice.docstore.api.mapping.DocPropertyMapping;
import io.ebeanservice.docstore.api.mapping.DocumentMapping;
import org.example.domain.Order;
import org.testng.annotations.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class DocMappingTest {

  @Test
  public void docMapping() {

    SpiEbeanServer server = (SpiEbeanServer) DB.getDefault();
    BeanDescriptor<Order> desc = server.descriptor(Order.class);

    DocumentMapping documentMapping = desc.docMapping();

    DocPropertyMapping properties = documentMapping.properties();

    assertThat(properties).isNotNull();
  }
}
