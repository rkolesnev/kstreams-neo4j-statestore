package io.github.rkolesnev.kstream.customstores.neo4j.streamsapp.pojo;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class DestinationMessage {
    private final String destinationUser;
    private final int distanceFromOrigin;
    private final String message;
}
