package io.github.rkolesnev.kstream.customstores.neo4j.statestore.changelogger;


import java.nio.charset.StandardCharsets;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.processor.internals.InternalProcessorContext;
import org.apache.kafka.streams.query.Position;

@RequiredArgsConstructor
public class ChangeLogger implements Neo4JChangeLogger {
  private final JsonSerializer serializer = new JsonSerializer();
  private final InternalProcessorContext context;
  private final String storeName;
  private final Supplier<Position> positionSupplier;

  @Override
  public void logChange(DeleteNodeOp deleteNodeOp) {
    Bytes key = Bytes.wrap(nodeOpKeyToChangelogNodeKey.apply(deleteNodeOp.getNodeKey()).getBytes(
        StandardCharsets.UTF_8));
    byte[] value = serializer.serialize(deleteNodeOp);
    context.logChange(storeName, key, value, context.timestamp(), positionSupplier.get());
    //Send tombstone for compaction to work.
    context.logChange(storeName, key, null, context.timestamp(), positionSupplier.get());
  }

  @Override
  public void logChange(DeleteRelationshipOp deleteRelationshipOp) {
    Bytes key = Bytes.wrap(
        relationshipOpKeyToChangelogKey.apply(deleteRelationshipOp.getRelationshipKey()).getBytes(
            StandardCharsets.UTF_8));
    byte[] value = serializer.serialize(deleteRelationshipOp);
    context.logChange(storeName, key, value, context.timestamp(), positionSupplier.get());
    //Send tombstone for compaction to work.
    context.logChange(storeName, key, null, context.timestamp(), positionSupplier.get());
  }

  @Override
  public void logChange(DeleteNodePropertyOp deleteNodePropertyOp) {
    Bytes key = Bytes.wrap(nodePropertyOpKeyToChangelogKey.apply(deleteNodePropertyOp.getNodeKey(),
        deleteNodePropertyOp.getPropertyKey()).getBytes(
        StandardCharsets.UTF_8));
    byte[] value = serializer.serialize(deleteNodePropertyOp);
    context.logChange(storeName, key, value, context.timestamp(), positionSupplier.get());
    //Send tombstone for compaction to work.
    context.logChange(storeName, key, null, context.timestamp(), positionSupplier.get());
  }

  @Override
  public void logChange(AddNodeOp addNodeOp) {
    Bytes key = Bytes.wrap(nodeOpKeyToChangelogNodeKey.apply(addNodeOp.getKey()).getBytes(
        StandardCharsets.UTF_8));
    byte[] value = serializer.serialize(addNodeOp);
    context.logChange(storeName, key, value, context.timestamp(), positionSupplier.get());
  }

  @Override
  public void logChange(SetNodePropertyOp setNodePropertyOp) {
    Bytes key = Bytes.wrap(nodePropertyOpKeyToChangelogKey.apply(setNodePropertyOp.getNodeKey(),
        setNodePropertyOp.getPropertyKey()).getBytes(
        StandardCharsets.UTF_8));
    byte[] value = serializer.serialize(setNodePropertyOp);
    context.logChange(storeName, key, value, context.timestamp(), positionSupplier.get());
  }

  @Override
  public void logChange(AddRelationshipOp addRelationshipOp) {
    Bytes key = Bytes.wrap(
        relationshipOpKeyToChangelogKey.apply(addRelationshipOp.getRelationshipKey()).getBytes(
            StandardCharsets.UTF_8));
    byte[] value = serializer.serialize(addRelationshipOp);
    context.logChange(storeName, key, value, context.timestamp(), positionSupplier.get());
  }

  @Override
  public void logChange(SetRelationshipPropertyOp setRelationshipPropertyOp) {
    Bytes key = Bytes.wrap(
        relationshipPropertyOpKeyToChangelogKey.apply(
            setRelationshipPropertyOp.getRelationshipKey(),
            setRelationshipPropertyOp.getPropertyKey()).getBytes(
            StandardCharsets.UTF_8));
    byte[] value = serializer.serialize(setRelationshipPropertyOp);
    context.logChange(storeName, key, value, context.timestamp(), positionSupplier.get());
  }

  @Override
  public void logChange(DeleteRelationshipPropertyOp deleteRelationshipPropertyOp) {
    Bytes key = Bytes.wrap(
        relationshipPropertyOpKeyToChangelogKey.apply(
            deleteRelationshipPropertyOp.getRelationshipKey(),
            deleteRelationshipPropertyOp.getPropertyKey()).getBytes(
            StandardCharsets.UTF_8));
    byte[] value = serializer.serialize(deleteRelationshipPropertyOp);
    context.logChange(storeName, key, value, context.timestamp(), positionSupplier.get());
    //Tombstone
    context.logChange(storeName, key, null, context.timestamp(), positionSupplier.get());
  }

  private final Function<String, String> nodeOpKeyToChangelogNodeKey = (key -> "N:" + key);
  private final BiFunction<String, String, String> nodePropertyOpKeyToChangelogKey = ((nodeKey, propKey) ->
      "NP:"
          + nodeKey + "_" + propKey);
  private final BiFunction<String, String, String> relationshipPropertyOpKeyToChangelogKey = ((relationshipKey, propKey) ->
      "RP:"
          + relationshipKey + "_" + propKey);
  private final Function<String, String> relationshipOpKeyToChangelogKey = (key -> "R:" + key);


}
