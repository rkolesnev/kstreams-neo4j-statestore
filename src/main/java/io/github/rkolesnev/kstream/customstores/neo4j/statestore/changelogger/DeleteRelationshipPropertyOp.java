package io.github.rkolesnev.kstream.customstores.neo4j.statestore.changelogger;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class DeleteRelationshipPropertyOp extends ChangeLogEntry {

  String relationshipKey;
  String relationshipType;
  String propertyKey;

  public DeleteRelationshipPropertyOp() {
    super(EntryType.DELETE_RELATIONSHIP_PROPERTY);
  }

  public DeleteRelationshipPropertyOp(String relationshipKey, String relationshipType,
      String propertyKey) {
    this();
    this.relationshipKey = relationshipKey;
    this.relationshipType = relationshipType;
    this.propertyKey = propertyKey;
  }
}
