package io.dexterity.entity.exchange;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class RangeQuery {

    private String bucket;
    private String pageNumber;
    private String pageSize;
    private String prefix;
    private String ub;
    private String lb;

}
