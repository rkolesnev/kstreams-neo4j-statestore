package io.github.rkolesnev.kstream.customstores.neo4j.streamsapp.pojo;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class PersonPojo {
  private String username;
  private String fullname;
  private int defaultFollowerDepth;
}
