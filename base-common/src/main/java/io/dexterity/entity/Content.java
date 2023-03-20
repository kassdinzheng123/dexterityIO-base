package io.dexterity.entity;

import lombok.Data;

@Data
public class Content {
    private String key;
    private String lastModified;
    private String eTag;
    private int size;
    private Owner owner;
    private String storageClass;
}
