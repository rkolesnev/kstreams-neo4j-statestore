package io.github.rkolesnev.kstream.customstores.neo4j.statestore.changelogger;


import lombok.Getter;

public abstract class ChangeLogEntry {
  @Getter
  private EntryType entryType;
  ChangeLogEntry(EntryType entryType){
    this.entryType = entryType;
  }
  public enum EntryType{
    ADD_NODE,
    ADD_RELATIONSHIP,
    SET_NODE_PROPERTY,
    SET_RELATIONSHIP_PROPERTY,
    DELETE_NODE,
    DELETE_RELATIONSHIP,
    DELETE_NODE_PROPERTY,
    DELETE_RELATIONSHIP_PROPERTY
  }

}
