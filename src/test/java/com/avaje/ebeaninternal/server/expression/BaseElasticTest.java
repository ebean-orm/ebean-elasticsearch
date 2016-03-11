package com.avaje.ebeaninternal.server.expression;


import com.avaje.ebean.Ebean;
import com.avaje.ebean.plugin.SpiServer;
import com.avaje.ebeaninternal.api.SpiQuery;
import com.avaje.ebeanservice.elastic.querywriter.ElasticJsonContext;
import com.avaje.ebeanservice.elastic.querywriter.ElasticDocQueryContext;

import java.io.IOException;

public abstract class BaseElasticTest  {

  protected static SpiServer server = Ebean.getDefaultServer().getPluginApi();

  public String asJson(SpiQuery<?> query) throws IOException {
    ElasticJsonContext context = new ElasticJsonContext(Ebean.json());
    return ElasticDocQueryContext.asJson(context, query);
  }

}