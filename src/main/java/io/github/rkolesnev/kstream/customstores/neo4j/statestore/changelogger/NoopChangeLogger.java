package io.github.rkolesnev.kstream.customstores.neo4j.statestore.changelogger;

/**
 * Can be used to disable change logging for the Neo4JStore.
 * Equivalent to building StateStore with logging set to false.
 */
public class NoopChangeLogger implements Neo4JChangeLogger{

  @Override
  public void logChange(DeleteNodeOp deleteNodeOp) {

  }

  @Override
  public void logChange(DeleteRelationshipOp deleteRelationshipOp) {

  }

  @Override
  public void logChange(DeleteNodePropertyOp deleteNodePropertyOp) {

  }

  @Override
  public void logChange(AddNodeOp addNodeOp) {

  }

  @Override
  public void logChange(SetNodePropertyOp setNodePropertyOp) {

  }

  @Override
  public void logChange(AddRelationshipOp addRelationshipOp) {

  }

  @Override
  public void logChange(SetRelationshipPropertyOp setRelationshipPropertyOp) {

  }

  @Override
  public void logChange(DeleteRelationshipPropertyOp deleteRelationshipPropertyOp) {

  }
}
