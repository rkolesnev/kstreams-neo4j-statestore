package io.github.rkolesnev.kstream.customstores.neo4j.statestore.changelogger;

import lombok.AccessLevel;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.FieldDefaults;

@FieldDefaults(level = AccessLevel.PRIVATE)
public class AddNodeOp extends ChangeLogEntry {

  @Getter
  @Setter
  String key;

  public AddNodeOp() {
    super(EntryType.ADD_NODE);
  }

  public AddNodeOp(String key) {
    this();
    this.key = key;
  }
}
