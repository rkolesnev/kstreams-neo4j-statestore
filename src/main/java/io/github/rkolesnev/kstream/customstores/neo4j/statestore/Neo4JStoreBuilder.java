package io.github.rkolesnev.kstream.customstores.neo4j.statestore;

import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.StoreSupplier;

public class Neo4JStoreBuilder implements StoreBuilder<Neo4jStore> {

  private final StoreSupplier<Neo4jStore> storeSupplier;
  private final Map<String, String> logConfig = new HashMap<>();

  public Neo4JStoreBuilder(final StoreSupplier<Neo4jStore> storeSupplier) {
    this.storeSupplier = storeSupplier;
  }

  @Override
  public StoreBuilder<Neo4jStore> withCachingEnabled() {
    //NOOP - caching not implemented for this store.
    return this;
  }

  @Override
  public StoreBuilder<Neo4jStore> withCachingDisabled() {
    //NOOP - caching not implemented for this store.
    return this;
  }

  @Override
  public StoreBuilder<Neo4jStore> withLoggingEnabled(Map<String, String> config) {
    //set logging config
    if (config != null && !config.isEmpty()) {
      logConfig.putAll(config);
    }
    //logging always enabled this store.
    return this;
  }

  @Override
  public StoreBuilder<Neo4jStore> withLoggingDisabled() {
    //NOOP - logging always enabled this store.
    return this;
  }

  @Override
  public Neo4jStore build() {
    return storeSupplier.get();
  }

  @Override
  public Map<String, String> logConfig() {
    return logConfig;
  }

  @Override
  public boolean loggingEnabled() {
    return true;
  }

  @Override
  public String name() {
    return this.storeSupplier.name();
  }
}
