package io.github.rkolesnev.kstream.customstores.neo4j.streamsapp.pojo;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class FollowPersonOp {
    String username;
    String usernameToFollow;
}
