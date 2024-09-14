package io.github.rkolesnev.kstream.customstores.neo4j.streamsapp;

import io.github.rkolesnev.kstream.customstores.neo4j.statestore.Neo4jStore;
import io.github.rkolesnev.kstream.customstores.neo4j.streamsapp.pojo.CreatePersonOp;
import io.github.rkolesnev.kstream.customstores.neo4j.streamsapp.pojo.DeletePersonOp;
import io.github.rkolesnev.kstream.customstores.neo4j.streamsapp.pojo.FollowPersonOp;
import io.github.rkolesnev.kstream.customstores.neo4j.streamsapp.pojo.NodeInputPojo;
import io.github.rkolesnev.kstream.customstores.neo4j.streamsapp.pojo.PersonPojo;
import io.github.rkolesnev.kstream.customstores.neo4j.streamsapp.pojo.UnfollowPersonOp;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.neo4j.graphdb.RelationshipType;

@Slf4j
public class NodeGraphProcessor implements
    Processor<String, NodeInputPojo, String, String> {

  private final String stateStoreName;
  private ProcessorContext<String, String> context;
  private Neo4jStore store;
  RelationshipType relationshipType;

  public NodeGraphProcessor(String stateStoreName, RelationshipType relationshipType) {
    this.stateStoreName = stateStoreName;
    this.relationshipType = relationshipType;
  }

  @Override
  public void init(final ProcessorContext<String, String> context) {
    this.context = context;
    this.store = context.getStateStore(stateStoreName);
  }

  @Override
  public void process(org.apache.kafka.streams.processor.api.Record<String, NodeInputPojo> record) {
    boolean success = switch (record.value().getOpType()) {
      case CREATE_PERSON -> createPerson(record.value().getCreatePersonOp());
      case DELETE_PERSON -> deletePerson(record.value().getDeletePersonOp());
      case FOLLOW_PERSON -> followPerson(record.value().getFollowPersonOp());
      case UNFOLLOW_PERSON -> unfollowPerson(record.value().getUnfollowPersonOp());
    };
    context.forward(new Record<>(record.key(),
        "Operation " + record.value().getOpType() + " executed " + (success
            ? "sucessfully" : "unsucessfully check logs for more information"),
        record.timestamp(), record.headers()));
  }

  private boolean unfollowPerson(UnfollowPersonOp unfollowPersonOp) {
    try {
      store.deleteRelationship(unfollowPersonOp.getUsername(),
          unfollowPersonOp.getUsernameToUnfollow(), relationshipType);
      return true;
    } catch (Exception e) {
      log.warn(
          "Error deleting relationship from Node with key:{} to Node with key:{} and relationshipType:{}. Exception: ",
          unfollowPersonOp.getUsername(), unfollowPersonOp.getUsernameToUnfollow(),
          relationshipType, e);
      return false;
    }
  }

  private boolean followPerson(FollowPersonOp followPersonOp) {
    try {
      store.relateNodes(followPersonOp.getUsername(),
          followPersonOp.getUsernameToFollow(),
          relationshipType, Map.of());
      return true;
    } catch (Exception e) {
      log.warn(
          "Error creating relationship from Node with key:{} to Node with key:{} and relationshipType:{}. Exception: ",
          followPersonOp.getUsername(), followPersonOp.getUsernameToFollow(),
          relationshipType, e);
      return false;
    }
  }

  private boolean deletePerson(DeletePersonOp deletePersonOp) {
    try {
      store.deleteNode(deletePersonOp.getUsername(), true);
      return true;
    } catch (Exception e) {
      log.warn("Error deleting Node with key:{} and force: true. Exception: ",
          deletePersonOp.getUsername(), e);
      return false;
    }
  }

  private boolean createPerson(CreatePersonOp createPersonOp) {
    PersonPojo personPojo = createPersonOp.getPersonPojo();
    String nodeKey = personPojo.getUsername();
    Map<String, String> properties = Map.of("fullname", personPojo.getFullname(),
        "defaultFollowerDepth",
        String.valueOf(personPojo.getDefaultFollowerDepth()));
    try {
      store.createNode(nodeKey, properties);
      return true;
    } catch (Exception e) {
      log.warn("Error creating Node with key:{} and properties:{}. Exception: ",
          nodeKey, properties, e);
      return false;
    }
  }
}