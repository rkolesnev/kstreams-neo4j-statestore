package io.github.rkolesnev.kstream.customstores.neo4j.streamsapp.pojo;

public class DestinationMessageJsonSerde extends JsonSerDe<DestinationMessage>{

  public DestinationMessageJsonSerde() {
    super(DestinationMessage.class);
  }
}
