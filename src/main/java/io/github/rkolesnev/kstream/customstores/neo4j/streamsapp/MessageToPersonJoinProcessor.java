package io.github.rkolesnev.kstream.customstores.neo4j.streamsapp;

import io.github.rkolesnev.kstream.customstores.neo4j.statestore.Neo4jStore;
import io.github.rkolesnev.kstream.customstores.neo4j.statestore.TraversalParameters;
import io.github.rkolesnev.kstream.customstores.neo4j.statestore.TraversalResult;
import io.github.rkolesnev.kstream.customstores.neo4j.streamsapp.pojo.DestinationMessage;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.kafka.streams.processor.api.FixedKeyProcessor;
import org.apache.kafka.streams.processor.api.FixedKeyProcessorContext;
import org.apache.kafka.streams.processor.api.FixedKeyRecord;
import org.neo4j.graphdb.RelationshipType;

import java.util.List;

public class MessageToPersonJoinProcessor implements FixedKeyProcessor<String, String, List<DestinationMessage>> {
    private final String stateStoreName;
    private final int searchDepth;
    private final RelationshipType relationshipType;
    FixedKeyProcessorContext<String, List<DestinationMessage>> context;
    Neo4jStore store;

    public MessageToPersonJoinProcessor(String stateStoreName, int searchDepth, RelationshipType relationshipType) {
        this.stateStoreName = stateStoreName;
        this.searchDepth = searchDepth;
        this.relationshipType = relationshipType;
    }

    public void init(
            final FixedKeyProcessorContext<String, List<DestinationMessage>> context) {
        this.context = context;
        this.store = context.getStateStore(stateStoreName);
    }

    @Override
    public void process(FixedKeyRecord<String, String> record) {
        String fromUser = record.key();
        String message = record.value();
        TraversalResult traversalResult = store.findRelatedNodes(fromUser,
                new TraversalParameters.TraversalParametersBuilder().withTraversalOrder(
                                TraversalParameters.TraversalOrder.BREADTH_FIRST).withMaxDepth(searchDepth)
                        .addRelationshipToTraverse(relationshipType,
                                TraversalParameters.RelationshipTraversalDirection.OUT).build());
        List<DestinationMessage> destinations = traversalResult.getFoundNodesWithLevel().entrySet()
                .stream()
                .flatMap(e -> e.getValue().stream().filter(v -> !fromUser.equals(v.key()))
                        .map(v -> new DestinationMessage(v.key(),e.getKey(), message)))
                .toList();
        if(!destinations.isEmpty()) {
            context.forward(record.withValue(destinations));
        }
    }
}
