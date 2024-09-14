package io.github.rkolesnev.kstream.customstores.neo4j.statestore;

import java.util.Map;

public record NodePojo(String key, Map<String, String> properties) {

}
