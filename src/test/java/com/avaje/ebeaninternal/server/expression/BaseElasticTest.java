package com.avaje.ebeaninternal.server.expression;


import com.avaje.ebean.Ebean;
import com.avaje.ebean.plugin.SpiServer;
import com.avaje.ebeaninternal.api.SpiQuery;
import com.avaje.ebeanservice.elastic.support.ElasticQueryContext;

import java.io.IOException;

public abstract class BaseElasticTest  {

  protected static SpiServer server = Ebean.getDefaultServer().getPluginApi();

  public String asJson(SpiQuery<?> query) throws IOException {
    return ElasticQueryContext.asJson(Ebean.json(), query);
  }

}