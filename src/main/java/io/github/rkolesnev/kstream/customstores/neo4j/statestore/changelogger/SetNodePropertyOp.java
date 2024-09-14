package io.github.rkolesnev.kstream.customstores.neo4j.statestore.changelogger;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class SetNodePropertyOp extends ChangeLogEntry {

  String nodeKey;
  String propertyKey;
  String propertyValue;

  public SetNodePropertyOp() {
    super(EntryType.SET_NODE_PROPERTY);
  }

  public SetNodePropertyOp(String nodeKey, String propertyKey, String propertyValue) {
    this();
    this.nodeKey = nodeKey;
    this.propertyKey = propertyKey;
    this.propertyValue = propertyValue;
  }
}
