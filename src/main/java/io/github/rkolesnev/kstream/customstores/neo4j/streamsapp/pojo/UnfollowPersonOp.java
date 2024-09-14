package io.github.rkolesnev.kstream.customstores.neo4j.streamsapp.pojo;

import lombok.Data;

@Data
public class UnfollowPersonOp {

  String username;
  String usernameToUnfollow;
}
