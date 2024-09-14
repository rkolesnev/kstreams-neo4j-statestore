package io.github.rkolesnev.kstream.customstores.neo4j.statestore;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import lombok.Data;

@Data
public class TraversalResult {
  private final Map<Integer, Set<NodePojo>> foundNodesWithLevel = new HashMap<>();

  public TraversalResult addNodeAtLevel(int level, NodePojo node) {
    foundNodesWithLevel.compute(level, (key, val) -> {
      Set<NodePojo> nodes = val != null ? val : new HashSet<>();
      nodes.add(node);
      return nodes;
    });
    return this;
  }
}
