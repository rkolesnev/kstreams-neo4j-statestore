package io.github.rkolesnev.kstream.customstores.neo4j.statestore;

import io.github.rkolesnev.kstream.customstores.neo4j.statestore.changelogger.AddNodeOp;
import io.github.rkolesnev.kstream.customstores.neo4j.statestore.changelogger.AddRelationshipOp;
import io.github.rkolesnev.kstream.customstores.neo4j.statestore.changelogger.ChangeLogEntry;
import io.github.rkolesnev.kstream.customstores.neo4j.statestore.changelogger.DeleteNodeOp;
import io.github.rkolesnev.kstream.customstores.neo4j.statestore.changelogger.DeleteNodePropertyOp;
import io.github.rkolesnev.kstream.customstores.neo4j.statestore.changelogger.DeleteRelationshipOp;
import io.github.rkolesnev.kstream.customstores.neo4j.statestore.changelogger.DeleteRelationshipPropertyOp;
import io.github.rkolesnev.kstream.customstores.neo4j.statestore.changelogger.JsonDeserializer;
import io.github.rkolesnev.kstream.customstores.neo4j.statestore.changelogger.SetNodePropertyOp;
import io.github.rkolesnev.kstream.customstores.neo4j.statestore.changelogger.SetRelationshipPropertyOp;
import java.util.Map;
import org.apache.kafka.streams.processor.StateRestoreCallback;
import org.neo4j.graphdb.RelationshipType;

public class Restorer implements StateRestoreCallback {

  private final JsonDeserializer jsonDeserializer = new JsonDeserializer();
  private final Neo4jStore store;

  public Restorer(Neo4jStore store) {
    this.store = store;
  }

  public void restore(byte[] key, byte[] value) {

    // ignore tombstones
    if (value == null) {
      return;
    }
    ChangeLogEntry entry = jsonDeserializer.deserialize(value);
    switch (entry.getEntryType()) {
      case ADD_NODE -> handleAddNode((AddNodeOp) entry);
      case ADD_RELATIONSHIP -> handleAddRelationship((AddRelationshipOp) entry);
      case SET_NODE_PROPERTY -> handleSetNodeProperty((SetNodePropertyOp) entry);
      case SET_RELATIONSHIP_PROPERTY ->
          handleSetRelationshipProperty((SetRelationshipPropertyOp) entry);
      case DELETE_NODE -> handleDeleteNode((DeleteNodeOp) entry);
      case DELETE_NODE_PROPERTY -> handleDeleteNodeProperty((DeleteNodePropertyOp) entry);
      case DELETE_RELATIONSHIP -> handleDeleteRelationship((DeleteRelationshipOp) entry);
      case DELETE_RELATIONSHIP_PROPERTY ->
          handleDeleteRelationshipProperty((DeleteRelationshipPropertyOp) entry);
      default -> throw new IllegalStateException(
          "Cannot handle restoration of changelog entry of type " + entry.getEntryType()
              + ". This entry type is unknown");
    }
  }

  private void handleDeleteRelationshipProperty(DeleteRelationshipPropertyOp entry) {
    store.removePropertyFromRelationship(RelationshipType.withName(entry.getRelationshipType()),
        entry.getRelationshipKey(), entry.getPropertyKey(), false);
  }

  private void handleDeleteRelationship(DeleteRelationshipOp entry) {
    store.deleteRelationship(entry.getRelationshipKey(), RelationshipType.withName(entry.getType()),
        false);
  }


  private void handleDeleteNodeProperty(DeleteNodePropertyOp entry) {
    store.removePropertyFromNode(entry.getNodeKey(), entry.getPropertyKey(), false);
  }


  private void handleDeleteNode(DeleteNodeOp entry) {
    store.deleteNode(entry.getNodeKey(), true, false);
  }

  private void handleSetRelationshipProperty(SetRelationshipPropertyOp entry) {
    store.addPropertyToRelationship(RelationshipType.withName(entry.getRelationshipType()),
        entry.getRelationshipKey(),
        entry.getPropertyKey(), entry.getPropertyValue(), false);
  }

  private void handleSetNodeProperty(SetNodePropertyOp entry) {
    store.setNodeProperty(entry.getNodeKey(), entry.getPropertyKey(), entry.getPropertyValue(),
        false);
  }


  private void handleAddRelationship(AddRelationshipOp entry) {
    store.relateNodes(entry.getStartNodeKey(), entry.getEndNodeKey(), entry.getRelationshipKey(),
        RelationshipType.withName(entry.getRelationshipType()),
        Map.of(), true, false);
    store.updateHighestSeenKey(Long.parseLong(entry.getRelationshipKey()));
  }

  private void handleAddNode(AddNodeOp entry) {
    store.createNode(entry.getKey(), Map.of(), false);
  }

}
