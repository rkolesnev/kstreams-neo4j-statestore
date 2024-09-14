package io.github.rkolesnev.kstream.customstores.neo4j.statestore;


import java.util.ArrayList;
import java.util.List;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.experimental.FieldDefaults;
import org.apache.commons.lang3.tuple.Pair;
import org.neo4j.graphdb.RelationshipType;

@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
@Getter
@FieldDefaults(level = AccessLevel.PRIVATE, makeFinal = true)
public class TraversalParameters {


  TraversalOrder traversalOrder;
  int maxTraversalDepth;
  List<Pair<RelationshipType, RelationshipTraversalDirection>> relationshipFilter;

  public static TraversalParametersBuilder builder() {
    return new TraversalParametersBuilder();
  }

  public static class TraversalParametersBuilder {

    private TraversalOrder traversalOrder = TraversalOrder.BREADTH_FIRST; //default to BREADTH_FIRST
    List<Pair<RelationshipType, RelationshipTraversalDirection>> relationshipFilter = new ArrayList<>();
    int maxTraversalDepth = -1;

    public TraversalParametersBuilder withTraversalOrder(TraversalOrder traversalOrder) {
      this.traversalOrder = traversalOrder;
      return this;
    }

    public TraversalParametersBuilder withMaxDepth(int maxTraversalDepth) {
      this.maxTraversalDepth = maxTraversalDepth;
      return this;
    }

    public TraversalParametersBuilder addRelationshipToTraverse(RelationshipType relationshipType,
        RelationshipTraversalDirection direction) {
      this.relationshipFilter.add(Pair.of(relationshipType, direction));
      return this;
    }

    public TraversalParameters build() {
      return new TraversalParameters(traversalOrder, maxTraversalDepth, relationshipFilter);
    }
  }

  public enum TraversalOrder {
    BREADTH_FIRST,
    DEPTH_FIRST
  }

  public enum RelationshipTraversalDirection {
    ANY,
    IN,
    OUT
  }
}
