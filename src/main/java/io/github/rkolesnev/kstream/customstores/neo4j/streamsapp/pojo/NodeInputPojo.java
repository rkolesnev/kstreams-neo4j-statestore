package io.github.rkolesnev.kstream.customstores.neo4j.streamsapp.pojo;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Builder
@Data
@AllArgsConstructor
@NoArgsConstructor
public class NodeInputPojo {

  OpType opType;
  CreatePersonOp createPersonOp;
  DeletePersonOp deletePersonOp;
  FollowPersonOp followPersonOp;
  UnfollowPersonOp unfollowPersonOp;

  public enum OpType {
    CREATE_PERSON,
    DELETE_PERSON,
    FOLLOW_PERSON,
    UNFOLLOW_PERSON
  }
}

