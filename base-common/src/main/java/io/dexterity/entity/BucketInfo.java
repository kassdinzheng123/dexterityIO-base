package io.dexterity.entity;

import io.dexterity.entity.xml.Owner;
import lombok.Data;

import java.util.ArrayList;
import java.util.List;

@Data
public class BucketInfo {
    private String name;
    private String creationDate;
    private Owner owner;
    private String region;
    private String acl;
    private String versioning;
    private String logging;
    private List<String> tagging = new ArrayList<>();
    private String cors;
    private LifeCycle lifecycle;
    private String notification;
    private String encryption;

    private String maxReader;
    private String metadataLimit;
}

