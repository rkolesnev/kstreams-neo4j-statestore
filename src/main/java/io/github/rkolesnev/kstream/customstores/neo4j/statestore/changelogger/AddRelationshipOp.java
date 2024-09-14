package io.github.rkolesnev.kstream.customstores.neo4j.statestore.changelogger;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class AddRelationshipOp extends ChangeLogEntry {
  private String startNodeKey;
  private String endNodeKey;
  private String relationshipType;
  private String relationshipKey;

  public AddRelationshipOp() {
    super(EntryType.ADD_RELATIONSHIP);
  }

  public AddRelationshipOp(String startNodeKey, String endNodeKey,
      String relationshipType, String relationshipKey) {
    this();
    this.startNodeKey = startNodeKey;
    this.endNodeKey = endNodeKey;
    this.relationshipType = relationshipType;
    this.relationshipKey = relationshipKey;
  }

}
