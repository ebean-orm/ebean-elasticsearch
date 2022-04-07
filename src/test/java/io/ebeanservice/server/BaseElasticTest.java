package io.ebeanservice.server;


import io.ebean.DB;
import io.ebean.Ebean;
import io.ebean.plugin.SpiServer;
import io.ebeaninternal.api.SpiQuery;
import io.ebeanservice.elastic.querywriter.ElasticJsonContext;
import io.ebeanservice.elastic.querywriter.ElasticDocQueryContext;

import java.io.IOException;

public abstract class BaseElasticTest  {

  protected static SpiServer server = DB.getDefault().pluginApi();

  public String asJson(SpiQuery<?> query) {
    ElasticJsonContext context = new ElasticJsonContext(DB.json());
    return ElasticDocQueryContext.asJson(context, query);
  }

}