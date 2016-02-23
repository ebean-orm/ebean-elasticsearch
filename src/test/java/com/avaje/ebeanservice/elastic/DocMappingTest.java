package com.avaje.ebeanservice.elastic;

import com.avaje.ebean.Ebean;
import com.avaje.ebeaninternal.api.SpiEbeanServer;
import com.avaje.ebeaninternal.server.deploy.BeanDescriptor;
import com.avaje.ebeanservice.docstore.api.mapping.DocPropertyMapping;
import com.avaje.ebeanservice.docstore.api.mapping.DocumentMapping;
import org.example.domain.Order;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class DocMappingTest {

  @Test
  public void docMapping() {

    SpiEbeanServer server = (SpiEbeanServer)Ebean.getDefaultServer();
    BeanDescriptor<Order> desc = server.getBeanDescriptor(Order.class);

    DocumentMapping documentMapping = desc.getDocMapping();

    DocPropertyMapping properties = documentMapping.getProperties();

    assertThat(properties).isNotNull();
  }
}
