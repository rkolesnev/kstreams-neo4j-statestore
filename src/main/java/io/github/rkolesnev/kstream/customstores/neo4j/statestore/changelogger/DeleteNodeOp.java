package io.github.rkolesnev.kstream.customstores.neo4j.statestore.changelogger;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class DeleteNodeOp extends ChangeLogEntry {


  String nodeKey;

  public DeleteNodeOp() {
    super(EntryType.DELETE_NODE);
  }

  public DeleteNodeOp(String nodeKey) {
    this();
    this.nodeKey = nodeKey;
  }
}
