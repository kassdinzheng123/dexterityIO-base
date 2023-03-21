package io.dexterity.entity.header;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class DexIOHost {
    private String region;
    private String bucketName;
    private String key;
}
