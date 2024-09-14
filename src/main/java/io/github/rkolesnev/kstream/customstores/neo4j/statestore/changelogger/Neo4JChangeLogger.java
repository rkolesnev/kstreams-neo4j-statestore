package io.github.rkolesnev.kstream.customstores.neo4j.statestore.changelogger;

public interface Neo4JChangeLogger {

  void logChange(DeleteNodeOp deleteNodeOp);

  void logChange(DeleteRelationshipOp deleteRelationshipOp);

  void logChange(DeleteNodePropertyOp deleteNodePropertyOp);

  void logChange(AddNodeOp addNodeOp);

  void logChange(SetNodePropertyOp setNodePropertyOp);

  void logChange(AddRelationshipOp addRelationshipOp);

  void logChange(SetRelationshipPropertyOp setRelationshipPropertyOp);

  void logChange(DeleteRelationshipPropertyOp deleteRelationshipPropertyOp);
}
