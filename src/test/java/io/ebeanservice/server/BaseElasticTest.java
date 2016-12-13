package io.ebeanservice.server;


import io.ebean.Ebean;
import io.ebean.plugin.SpiServer;
import io.ebeaninternal.api.SpiQuery;
import io.ebeanservice.elastic.querywriter.ElasticJsonContext;
import io.ebeanservice.elastic.querywriter.ElasticDocQueryContext;

import java.io.IOException;

public abstract class BaseElasticTest  {

  protected static SpiServer server = Ebean.getDefaultServer().getPluginApi();

  public String asJson(SpiQuery<?> query) throws IOException {
    ElasticJsonContext context = new ElasticJsonContext(Ebean.json());
    return ElasticDocQueryContext.asJson(context, query);
  }

}