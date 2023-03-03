package io.dexterity.client.entity;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@NoArgsConstructor
@AllArgsConstructor
@Data
public class LMDBEnvSettings {
    private int maxSize;
    private int maxDBInstance;
    private int maxReaders;
    private String filePosition;
    private String envName;
}
