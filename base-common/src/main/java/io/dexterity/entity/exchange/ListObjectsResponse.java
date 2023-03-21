package io.dexterity.entity.exchange;

import io.dexterity.entity.xml.Content;

import java.util.List;

public class ListObjectsResponse {
    private List<Content> content;
    private String encodingType;
    private String name;
    private String prefix;
    private boolean isTruncated;
    private int keyCount;
}
