package io.github.rkolesnev.kstream.customstores.neo4j.statestore;

import java.util.Map;
import lombok.Data;

@Data
public class RelationshipPojo {
  final String relationshipKey;
  final String relationshipType;
  final String startNodeKey;
  final String endNodeKey;
  final Map<String,String> properties;
  final Direction direction;

  public enum Direction{
    IN,
    OUT
  }
}

