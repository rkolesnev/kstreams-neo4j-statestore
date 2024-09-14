package io.github.rkolesnev.kstream.customstores.neo4j.statestore.changelogger;

import lombok.Getter;
import lombok.Setter;
import org.apache.kafka.common.protocol.types.Field.Str;

@Getter
@Setter
public class SetRelationshipPropertyOp extends ChangeLogEntry {

  String relationshipKey;
  String relationshipType;
  String propertyKey;
  String propertyValue;

  public SetRelationshipPropertyOp() {
    super(EntryType.SET_RELATIONSHIP_PROPERTY);
  }

  public SetRelationshipPropertyOp(String relationshipKey, String relationshipType,
      String propertyKey, String propertyValue) {
    this();
    this.relationshipKey = relationshipKey;
    this.relationshipType = relationshipType;
    this.propertyKey = propertyKey;
    this.propertyValue = propertyValue;
  }
}
