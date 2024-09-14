package io.github.rkolesnev.kstream.customstores.neo4j.statestore;

import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.graphdb.Direction.BOTH;
import static org.neo4j.graphdb.Direction.INCOMING;
import static org.neo4j.graphdb.Direction.OUTGOING;

import com.google.common.annotations.VisibleForTesting;
import io.github.rkolesnev.kstream.customstores.neo4j.statestore.RelationshipPojo.Direction;
import io.github.rkolesnev.kstream.customstores.neo4j.statestore.TraversalParameters.RelationshipTraversalDirection;
import io.github.rkolesnev.kstream.customstores.neo4j.statestore.TraversalParameters.TraversalOrder;
import io.github.rkolesnev.kstream.customstores.neo4j.statestore.changelogger.AddNodeOp;
import io.github.rkolesnev.kstream.customstores.neo4j.statestore.changelogger.AddRelationshipOp;
import io.github.rkolesnev.kstream.customstores.neo4j.statestore.changelogger.DeleteNodeOp;
import io.github.rkolesnev.kstream.customstores.neo4j.statestore.changelogger.DeleteRelationshipPropertyOp;
import io.github.rkolesnev.kstream.customstores.neo4j.statestore.changelogger.Neo4JChangeLogger;
import io.github.rkolesnev.kstream.customstores.neo4j.statestore.changelogger.SetNodePropertyOp;
import io.github.rkolesnev.kstream.customstores.neo4j.statestore.changelogger.ChangeLogger;
import io.github.rkolesnev.kstream.customstores.neo4j.statestore.changelogger.DeleteRelationshipOp;
import io.github.rkolesnev.kstream.customstores.neo4j.statestore.changelogger.DeleteNodePropertyOp;
import io.github.rkolesnev.kstream.customstores.neo4j.statestore.changelogger.SetRelationshipPropertyOp;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.UUID;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.kafka.streams.errors.ProcessorStateException;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.StateStoreContext;
import org.apache.kafka.streams.processor.internals.InternalProcessorContext;
import org.apache.kafka.streams.processor.internals.RecordBatchingStateRestoreCallback;
import org.apache.kafka.streams.processor.internals.StoreToProcessorContextAdapter;
import org.apache.kafka.streams.query.Position;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.FSDirectory;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.configuration.connectors.BoltConnector;
import org.neo4j.configuration.helpers.SocketAddress;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.dbms.api.DatabaseManagementServiceBuilder;
import org.neo4j.graphdb.Entity;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.ResourceIterable;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.traversal.Evaluators;
import org.neo4j.graphdb.traversal.TraversalDescription;
import org.neo4j.graphdb.traversal.Traverser;
import org.neo4j.io.ByteUnit;
import org.rocksdb.util.Environment;

@Slf4j
public class Neo4jStore implements StateStore {

  DatabaseManagementService managementService;
  GraphDatabaseService graphDb;
  @Getter
  String nodeKeyProperty = "nkey"; //default to "nkey"
  @Getter
  String relationshipKeyProperty = "rkey";
  Label nodeLabel = Label.label(
      "default"); //label to use for all nodes - defaults to "default" - will be used for indexing potentially.
  Predicate<RelationshipType> isRelationshipTypeSupported = (relationshipType) -> true; //default - no restriction on relationship types
  Neo4JChangeLogger changeLogger;
  private long key = 0;

  File parentDir;
  String name;

  ProcessorContext context;
  private boolean isOpen;

  public Neo4jStore(String name) {
    this.name = name;
  }

  @VisibleForTesting
  Neo4jStore(Neo4JChangeLogger changeLogger, Path parentDir) {
    this.name = "neo4j-" + UUID.randomUUID();
    this.changeLogger = changeLogger;
    Path databasePath = Path.of(parentDir.toAbsolutePath().toString(), name);
    init(databasePath);
  }

  @Override
  public String name() {
    return this.name;
  }

  @Override
  public void init(ProcessorContext context, StateStore root) {
    this.parentDir = context.stateDir();

    try {
      Files.createDirectories(parentDir.toPath());
      Files.createDirectories(parentDir.getAbsoluteFile().toPath());
    } catch (final IOException fatal) {
      throw new ProcessorStateException(fatal);
    }

    Path databaseDirectory = Path.of(parentDir.getAbsolutePath(), name);

    init(databaseDirectory);
    Restorer restorer = new Restorer(this);
    if (root != null) {
      context.register(
          root,
          restorer);
    }
    //Allow to keep changelogger set from test visible constructor
    if (this.changeLogger == null) {
      //Position tracking is not implemented.
      this.changeLogger = new ChangeLogger((InternalProcessorContext) context, name,
          () -> Position.emptyPosition());
    }
    this.context = context;
  }

  @Override
  public void init(StateStoreContext context, StateStore root) {
    ProcessorContext contextAsProcessorContext = StoreToProcessorContextAdapter.adapt(context);
    init(contextAsProcessorContext, root);
  }

  private void init(Path databaseDirectory) {
    //TODO: hook the path based on store name
    // checks for path / restoration tracking.

    //Bolt connector enables remote connection from visualization tools (e.g. Neo4jBrowser)
    BoltConnector connector = new BoltConnector();
    managementService = new DatabaseManagementServiceBuilder(
        databaseDirectory)
        .setConfig(BoltConnector.enabled, true)
        .setConfig(BoltConnector.listen_address, new SocketAddress("localhost",7688))
        .setConfig(GraphDatabaseSettings.pagecache_memory, ByteUnit.mebiBytes(512))
        .setConfig(GraphDatabaseSettings.transaction_timeout, Duration.ofSeconds(60))
        .setConfig(GraphDatabaseSettings.preallocate_logical_logs, true)
        .build();
    graphDb = managementService.database(DEFAULT_DATABASE_NAME);

    //TODO: Maybe index creation if not exist on node Key
    try (Transaction tx = graphDb.beginTx()) {
      tx.schema().constraintFor(nodeLabel).assertPropertyIsUnique(nodeKeyProperty).create();
      tx.schema().constraintFor(RelationshipType.withName("rel"))
          .assertPropertyIsUnique(relationshipKeyProperty).create();
      tx.commit();
    }
    isOpen = true;
  }

  public NodePojo createNode(String key, Map<String, String> properties) {
    return createNode(key, properties, true);
  }

  NodePojo createNode(String key, Map<String, String> properties, boolean logChange) {
    Node node;
    try (Transaction tx = graphDb.beginTx()) {
      node = createNode(key, properties, tx);
      tx.commit();
    }
    if (logChange) {
      changeLogger.logChange(new AddNodeOp(key));
      properties.forEach((propertyKey, propertyValue) ->
          changeLogger.logChange(new SetNodePropertyOp(key, propertyKey, propertyValue)));
    }
    return new NodePojo(key, properties);
  }

  private Node createNode(String key, Map<String, String> properties, Transaction tx) {
    if (properties == null) {
      properties = Map.of();
    }
    if (properties.containsKey(nodeKeyProperty)) {
      throw new IllegalArgumentException(
          "Additional node properties should not redefine node key property configured as "
              + nodeKeyProperty + " for this state store instance");
    }
    Node node = tx.createNode(nodeLabel);
    node.setProperty(nodeKeyProperty, key);
    properties.forEach((propKey, propValue) -> setNodeProperty(node, propKey, propValue, tx));
    return node;
  }

  public void setNodeProperty(String nodeKey, String propertyKey, String propertyValue) {
    setNodeProperty(nodeKey, propertyKey, propertyValue, true);
  }

  void setNodeProperty(String nodeKey, String propertyKey, String propertyValue,
      boolean logChange) {
    try (Transaction tx = graphDb.beginTx()) {
      Node node = getNodeByKey(nodeKey, tx);
      if (node == null) {
        return;
      }
      setNodeProperty(node, propertyKey, propertyValue, tx);
      tx.commit();
    }
    if (logChange) {
      changeLogger.logChange(new SetNodePropertyOp(nodeKey, propertyKey, propertyValue));
    }
  }

  private void setNodeProperty(Node node, String propertyKey,
      String propertyValue, Transaction tx) {
    node.setProperty(propertyKey, propertyValue);
  }

  public NodePojo getNodeByKey(String nodeKey) {
    try (Transaction tx = graphDb.beginTx()) {
      Node node = getNodeByKey(nodeKey, tx);
      if (node != null) {
        return new NodePojo(nodeKey,
            convertProperties(node.getAllProperties()));
      } else {
        return null;
      }
    }
  }

  public List<RelationshipPojo> getNodeRelationships(String nodeKey) {
    try (Transaction tx = graphDb.beginTx()) {
      Node searchNode = getNodeByKey(nodeKey, tx);
      ResourceIterable<Relationship> relationships = searchNode.getRelationships();
      List<Relationship> relationshipList = relationships.stream().toList();
      relationships.close();

      return relationshipList.stream().map(relationship -> {
        Direction direction;
        String startNodeKey;
        String endNodeKey;
        if (relationship.getEndNode().getProperty(nodeKeyProperty).equals(nodeKey)) {
          direction = Direction.IN;
          endNodeKey = nodeKey;
          startNodeKey = (String) relationship.getOtherNode(searchNode)
              .getProperty(nodeKeyProperty);
        } else {
          direction = Direction.OUT;
          startNodeKey = nodeKey;
          endNodeKey = (String) relationship.getEndNode().getProperty(nodeKeyProperty);
        }

        String relType = relationship.getType().name();
        Map<String, Object> allProps = relationship.getAllProperties();
        String relKey = (String) allProps.get(relationshipKeyProperty);
        Map<String, String> relProperties = convertProperties(allProps);
        return new RelationshipPojo(relKey, relType, startNodeKey, endNodeKey, relProperties,
            direction);
      }).toList();
    }
  }

  public TraversalResult findRelatedNodes(String nodeKey, TraversalParameters traversalParameters) {
    try (Transaction tx = graphDb.beginTx()) {
      TraversalDescription td;
      if (traversalParameters.getTraversalOrder() == TraversalOrder.BREADTH_FIRST) {
        td = tx.traversalDescription().breadthFirst();
      } else {
        td = tx.traversalDescription().depthFirst();
      }
      for (Pair<RelationshipType, RelationshipTraversalDirection> relationshipFilterEntry : traversalParameters.getRelationshipFilter()) {
        org.neo4j.graphdb.Direction relationshipTraversalDirection = getRelationshipTraversalDirection(
            relationshipFilterEntry);
        td = td.relationships(relationshipFilterEntry.getLeft(), relationshipTraversalDirection);
      }
      if (traversalParameters.getMaxTraversalDepth() > 0) {
        td = td.evaluator(Evaluators.toDepth(traversalParameters.getMaxTraversalDepth()));
      }
      td = td.evaluator(Evaluators.excludeStartPosition());

      Traverser traverser = td.traverse(getNodeByKey(nodeKey, tx));
      TraversalResult traversalResult = new TraversalResult();
      traverser.iterator().forEachRemaining(result -> {
        Map<String,Object> allProperties =result.endNode().getAllProperties();
        String resultNodeKey = (String)allProperties.get(nodeKeyProperty);
        Map<String, String> nodeProperties = convertProperties(allProperties);
        traversalResult.addNodeAtLevel(result.length(),
            new NodePojo(resultNodeKey,
                nodeProperties));
      });
      return traversalResult;
    }
  }

  private Map<String, String> convertProperties(Map<String, Object> properties) {
    if (properties == null || properties.isEmpty()) {
      return Map.of();
    } else {
      return properties.entrySet().stream().filter(
              e -> !(e.getKey().equals(nodeKeyProperty) || e.getKey().equals(relationshipKeyProperty)))
          .collect(Collectors.toMap(Entry::getKey, e -> (String) e.getValue()));
    }
  }

  public boolean deleteNode(String nodeKey, boolean force) {
    return deleteNode(nodeKey, force, true);
  }

  boolean deleteNode(String nodeKey, boolean force, boolean logChange) {
    try (Transaction tx = graphDb.beginTx()) {
      Node node = getNodeByKey(nodeKey, tx);
      if (node == null) {
        return false;
      }
      Set<Pair<DeleteRelationshipOp, List<DeleteRelationshipPropertyOp>>> relationshipDeleteDataForChangelog = null;
      if (node.hasRelationship()) {
        if (force) {
          //Delete all relationships before deleting the node as delete is forced.
          ResourceIterable<Relationship> relationships =
              node.getRelationships();
          List<Relationship> relationshipList = relationships.stream().toList();
          relationships.close();
          relationshipDeleteDataForChangelog =
              relationshipList.stream().map(relationship -> {
                List<DeleteRelationshipPropertyOp> relPropDeletes = new ArrayList<>();
                String relationshipKey = (String) relationship.getProperty(relationshipKeyProperty);
                convertProperties(relationship.getAllProperties()).forEach((k, v) -> {
                  relPropDeletes.add(
                      new DeleteRelationshipPropertyOp(relationshipKey,
                          relationship.getType().name(), k));
                });
                return Pair.of(
                    new DeleteRelationshipOp(relationshipKey, relationship.getType().name()),
                    relPropDeletes);
              }).collect(Collectors.toSet());
          relationshipList.forEach(Entity::delete);
        } else {
          return false;
        }
      }

      Map<String, String> nodeProperties = convertProperties(node.getAllProperties());
      nodeProperties.remove(nodeKeyProperty);
      node.delete();
      tx.commit();
      if (logChange) {
        if (relationshipDeleteDataForChangelog != null
            && !relationshipDeleteDataForChangelog.isEmpty()) {
          relationshipDeleteDataForChangelog.forEach(pair -> {
            pair.getRight().forEach(deleteProp -> changeLogger.logChange(deleteProp));
            changeLogger.logChange(pair.getLeft());
          });
        }
        nodeProperties.forEach(
            (k, v) -> changeLogger.logChange(new DeleteNodePropertyOp(nodeKey, k)));
        changeLogger.logChange(new DeleteNodeOp(nodeKey));
      }
      return true;
    }
  }

  public void deleteRelationship(String fromNodeKey, String toNodeKey,
      RelationshipType relationshipType) {
    String relationshipKey = getNodeRelationships(fromNodeKey).stream().filter(
            relationshipPojo -> relationshipPojo.getEndNodeKey().equals(toNodeKey)
                && relationshipPojo.getDirection() == Direction.OUT
                && relationshipPojo.getRelationshipType().equals(relationshipType.name())).findFirst()
        .map(
            RelationshipPojo::getRelationshipKey).orElse(null);
    if (relationshipKey == null) {
      throw new IllegalArgumentException(
          "Relationship with type: " + relationshipType.name() + " does not exist for fromNodeKey:"
              + fromNodeKey + " toNodeKey:" + toNodeKey);
    }
    deleteRelationship(relationshipKey, relationshipType, true);
  }

  public void deleteRelationship(String relationshipKey, RelationshipType relationshipType) {
    deleteRelationship(relationshipKey, relationshipType, true);
  }

  void deleteRelationship(String relationshipKey, RelationshipType relationshipType,
      boolean logChange) {
    try (Transaction tx = graphDb.beginTx()) {
      Relationship relationship = getRelationshipByKey(relationshipType, relationshipKey, tx);
      Map<String, String> relationshipProperties = convertProperties(
          relationship.getAllProperties());
      boolean deleted = deleteRelationship(relationshipKey, relationshipType, tx);
      if (deleted) {
        tx.commit();
      }

      if (deleted && logChange) {
        relationshipProperties.forEach(
            (k, v) -> changeLogger.logChange(
                new DeleteRelationshipPropertyOp(relationshipKey, relationshipType.name(),
                    k)));
        changeLogger.logChange(
            new DeleteRelationshipOp(relationshipKey, relationshipType.name()));
      }
    }
  }

  private boolean deleteRelationship(String relationshipKey, RelationshipType relationshipType,
      Transaction tx) {
    Relationship relationship = getRelationshipByKey(relationshipType, relationshipKey, tx);
    if (relationship == null) {
      return false;
    }
    relationship.delete();
    return true;
  }

  public String removePropertyFromNode(String nodeKey, String propertyToRemove) {
    return removePropertyFromNode(nodeKey, propertyToRemove, true);
  }

  String removePropertyFromNode(String nodeKey, String propertyToRemove, boolean logChange) {
    try (Transaction tx = graphDb.beginTx()) {
      Node node = getNodeByKey(nodeKey, tx);
      if (node == null) {
        return null;
      }
      String result = (String) node.removeProperty(propertyToRemove);
      if (result != null) {
        tx.commit();
        if (logChange) {
          changeLogger.logChange(new DeleteNodePropertyOp(nodeKey, propertyToRemove));
        }
      }
      return result;
    }
  }

  public void addPropertyToRelationship(RelationshipType relationshipType,
      String relationshipKey, String propertyKey, String propertyValue) {
    addPropertyToRelationship(relationshipType, relationshipKey, propertyKey, propertyValue, true);
  }

  void addPropertyToRelationship(RelationshipType relationshipType,
      String relationshipKey, String propertyKey, String propertyValue, boolean logChange) {
    try (Transaction tx = graphDb.beginTx()) {
      boolean added = addPropertyToRelationship(relationshipType, relationshipKey, propertyKey,
          propertyValue, tx);
      tx.commit();
      if (added && logChange) {
        changeLogger.logChange(
            new SetRelationshipPropertyOp(relationshipKey, relationshipType.name(), propertyKey,
                propertyValue));
      }
    }
  }

  private boolean addPropertyToRelationship(RelationshipType relationshipType,
      String relationshipKey, String propertyKey, String propertyValue, Transaction tx) {
    Relationship relationship = getRelationshipByKey(relationshipType, relationshipKey, tx);
    if (relationship != null) {
      relationship.setProperty(propertyKey, propertyValue);
    }
    return relationship != null;
  }

  private Relationship getRelationshipByKey(RelationshipType relationshipType,
      String relationshipKey, Transaction tx) {
    return tx.findRelationship(relationshipType, relationshipKeyProperty, relationshipKey);
  }

  public String removePropertyFromRelationship(RelationshipType relationshipType,
      String relationshipKey, String propertyToRemove) {
    return removePropertyFromRelationship(relationshipType, relationshipKey, propertyToRemove,
        true);
  }

  String removePropertyFromRelationship(RelationshipType relationshipType,
      String relationshipKey, String propertyToRemove, boolean logChange) {
    try (Transaction tx = graphDb.beginTx()) {

      String result = removePropertyFromRelationship(relationshipType, relationshipKey,
          propertyToRemove, tx);
      if (result != null) {
        tx.commit();
        if (logChange) {
          changeLogger.logChange(
              new DeleteRelationshipPropertyOp(relationshipKey, relationshipType.name(),
                  propertyToRemove));
        }
      }
      return result;
    }
  }

  private String removePropertyFromRelationship(RelationshipType relationshipType,
      String relationshipKey, String propertyToRemove, Transaction tx) {
    Relationship relationship = getRelationshipByKey(relationshipType, relationshipKey, tx);
    if (relationship == null) {
      return null;
    }
    return (String) relationship.removeProperty(propertyToRemove);
  }

  private static org.neo4j.graphdb.Direction getRelationshipTraversalDirection(
      Pair<RelationshipType, RelationshipTraversalDirection> relationshipFilterEntry) {
    org.neo4j.graphdb.Direction relationshipTraversalDirection = BOTH;
    if (relationshipFilterEntry.getRight() != null) {
      relationshipTraversalDirection =
          switch (relationshipFilterEntry.getRight()) {
            case ANY -> BOTH;
            case IN -> INCOMING;
            case OUT -> OUTGOING;
            default -> throw new IllegalArgumentException(
                //Shouldn't really ever happen unless new traversal direction added to RelationshipTraversalDirection enum and this is not updated to reflect.
                "Relationship supplied in Traversal Parameters with unrecognized RelationshipTraversalDirection of "
                    + relationshipFilterEntry.getRight().name());
          };
    }
    return relationshipTraversalDirection;
  }

  private Node getNodeByKey(String nodeKey, Transaction tx) {
    try (ResourceIterator<Node> nodes = tx.findNodes(nodeLabel, nodeKeyProperty, nodeKey)) {
      Node node = null;
      if (nodes.hasNext()) {
        node = nodes.next();
      }
      log.trace("getNodeByKey with nodeKey: {}, returning node: {}", nodeKey,
          node != null ? node : "null");
      return node;
    }
  }

  public RelationshipPojo relateNodes(String nodeFromKey, String nodeToKey,
      RelationshipType relationshipType, Map<String, String> relationshipProperties) {
    return relateNodes(nodeFromKey, nodeToKey, relationshipType, relationshipProperties, false);
  }

  public RelationshipPojo relateNodes(String nodeFromKey, String nodeToKey,
      RelationshipType relationshipType, Map<String, String> relationshipProperties,
      boolean skipOnMissingNodes) {
    return relateNodes(nodeFromKey, nodeToKey, relationshipType, relationshipProperties,
        skipOnMissingNodes, true);
  }

  RelationshipPojo relateNodes(String nodeFromKey, String nodeToKey,
      RelationshipType relationshipType, Map<String, String> relationshipProperties,
      boolean skipOnMissingNodes, boolean logChange) {
    try (Transaction tx = graphDb.beginTx()) {
      Node from = getNodeByKey(nodeFromKey, tx);
      Node to = getNodeByKey(nodeToKey, tx);
      if (from == null || to == null) {
        if (skipOnMissingNodes) {
          return null;
        } else {
          throw new IllegalArgumentException(
              (from == null ? "From" : "To") + " node with key:" + (from == null ? nodeFromKey
                  : nodeToKey) + " could not be found.");
        }
      }

      String relationshipKey = "" + nextKey();
      Relationship relationship = relateNodes(from, to, relationshipType, relationshipProperties,
          relationshipKey);

      tx.commit();
      if (logChange) {
        changeLogger.logChange(
            new AddRelationshipOp(nodeFromKey, nodeToKey, relationshipType.name(),
                relationshipKey));
        if (relationshipProperties != null && !relationshipProperties.isEmpty()) {
          relationshipProperties.forEach((k, v) -> changeLogger.logChange(
              new SetRelationshipPropertyOp(relationshipKey, relationshipType.name(), k, v)));
        }
      }
      return new RelationshipPojo(relationshipKey, relationshipType.name(), nodeFromKey, nodeToKey,
          relationshipProperties, Direction.OUT);
    }
  }

  private long nextKey() {
    key++;
    return key;
  }

  private void updateHighestSeenKey(long incomingKey) {
    if (incomingKey > key) {
      key = incomingKey;
    }
  }

  /**
   * Has to be executed within running transaction and transaction has to be committed.
   */
  private Relationship relateNodes(Node nodeFrom, Node nodeTo, RelationshipType relationshipType,
      Map<String, String> relationshipProperties, String relationshipKey) {
    Relationship relationship = nodeFrom.createRelationshipTo(nodeTo, relationshipType);
    if (relationshipProperties != null && !relationshipProperties.isEmpty()) {
      relationshipProperties.forEach(relationship::setProperty);
    }
    relationship.setProperty(relationshipKeyProperty, relationshipKey);
    return relationship;
  }

  void doStuff() {
    Node firstNode;
    Node secondNode;
    Relationship relationship;
    Label label = Label.label("nodeLabel");
    try (Transaction tx = graphDb.beginTx()) {
      tx.schema().constraintFor(label).assertPropertyIsUnique("message").create();
      tx.commit();
    }
    try (Transaction tx = graphDb.beginTx()) {

      firstNode = tx.createNode();
      firstNode.addLabel(label);
      firstNode.setProperty("message", "Hello, ");

      secondNode = tx.createNode();
      secondNode.addLabel(label);
      secondNode.setProperty("message", "World!");
      tx.commit();

    }

    try (Transaction tx = graphDb.beginTx()) {
      firstNode = tx.findNode(label, "message", "Hello, ");
      relationship = firstNode.createRelationshipTo(secondNode, RelTypes.KNOWS);
      relationship.setProperty("message", "brave Neo4j ");
      tx.commit();
    }
    try (Transaction tx = graphDb.beginTx()) {
      ResourceIterator<Node> nodes = tx.findNodes(label);
      while (nodes.hasNext()) {
        Node node = nodes.next();
        log.info("Node found: {}", node.getProperty("message"));
        System.out.println("Node relationships: ");
        ResourceIterable<Relationship> rels = node.getRelationships();
        for (Relationship rel : rels) {
          log.info(
              "\trelationship: type: {}, start: {}, end: {}", rel.getType(), rel.getStartNode(),
              rel.getEndNode());
        }
        rels.close();
      }
      nodes.close();
    }
  }

  private enum RelTypes implements RelationshipType {
    KNOWS
  }

  @Override
  public void flush() {
    //Noop - all operations should commit their own transactions
  }

  @Override
  public void close() {
    this.isOpen = false;
    managementService.shutdown();
  }

  @Override
  public boolean persistent() {
    //Implement flag and logic to wipe state if noop changelogger is to be used / store is planned to be used as non-persistent store.
    return true;
  }

  @Override
  public boolean isOpen() {
    return isOpen;
  }
}