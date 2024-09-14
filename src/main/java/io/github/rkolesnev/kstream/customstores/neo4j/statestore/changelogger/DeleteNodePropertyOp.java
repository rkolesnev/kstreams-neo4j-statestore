package io.github.rkolesnev.kstream.customstores.neo4j.statestore.changelogger;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class DeleteNodePropertyOp extends ChangeLogEntry {

  String nodeKey;
  String propertyKey;

  public DeleteNodePropertyOp() {
    super(EntryType.DELETE_NODE_PROPERTY);
  }

  public DeleteNodePropertyOp(String nodeKey, String propertyKey) {
    this();
    this.nodeKey = nodeKey;
    this.propertyKey = propertyKey;
  }
}
