package io.github.rkolesnev.kstream.customstores.neo4j.statestore.changelogger;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class DeleteRelationshipOp extends ChangeLogEntry {

  String relationshipKey;
  String type;

  public DeleteRelationshipOp() {
    super(EntryType.DELETE_RELATIONSHIP);
  }

  public DeleteRelationshipOp(String relationshipKey, String type) {
    this();
    this.relationshipKey = relationshipKey;
    this.type = type;
  }
}
