package io.github.rkolesnev.kstream.customstores.neo4j.streamsapp.pojo;

public class NodeInputJsonSerde extends JsonSerDe<NodeInputPojo>{

  public NodeInputJsonSerde() {
    super(NodeInputPojo.class);
  }
}
