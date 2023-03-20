package io.dexterity.entity;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class ListObjectsRequest {
    private String delimiter;
    private String encodingType;
    private String maxKeys;
    private String prefix;
    private String fetchOwner;
    private String lb;
    private String ub;
}