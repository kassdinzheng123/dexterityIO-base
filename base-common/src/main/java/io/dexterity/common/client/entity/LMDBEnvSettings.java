package io.dexterity.common.client.entity;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@NoArgsConstructor
@AllArgsConstructor
@Data
public class LMDBEnvSettings {
    private Long maxSize;
    private int maxDBInstance;
    private int maxReaders;
    private String filePosition;
    private String envName;
    private String secretKey = "ABC-CDQ-QDF-AZX-SDF";
}
