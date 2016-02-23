package org.example;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;

/**
 */
public class EmbeddedElasticServer {


  final Node node;

  public EmbeddedElasticServer() {

    Settings settings1 = Settings.settingsBuilder()
        .put("path.home", "target/elastic-data")
        .put("number_of_shards", "1")
        .put("number_of_replicas", "1")
        .build();


    this.node = NodeBuilder
        .nodeBuilder()
        .settings(settings1)
        .node();
  }


  public Node getNode() {
    return node;
  }

  public void stop() {
    node.close();
  }

}
