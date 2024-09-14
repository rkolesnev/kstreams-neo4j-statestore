package io.github.rkolesnev.kstream.customstores.neo4j.statestore;

import org.apache.kafka.streams.state.StoreSupplier;

public class Neo4jStoreSupplier implements StoreSupplier<Neo4jStore> {

  private final String name;

  public Neo4jStoreSupplier(String name) {
    this.name = name;
  }

  @Override
  public String name() {
    return name;
  }

  @Override
  public Neo4jStore get() {
    return new Neo4jStore(name);
  }

  @Override
  public String metricsScope() {
    //TODO: implement for metrics
    return "neo4jstore-not-implemented";
  }
}
