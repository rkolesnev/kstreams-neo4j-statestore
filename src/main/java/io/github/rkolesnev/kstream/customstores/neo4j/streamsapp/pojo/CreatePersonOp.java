package io.github.rkolesnev.kstream.customstores.neo4j.streamsapp.pojo;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@AllArgsConstructor
@NoArgsConstructor
@Data
public class CreatePersonOp {
    PersonPojo personPojo;
}
