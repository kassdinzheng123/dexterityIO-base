package io.dexterity.entity;

import lombok.Data;

@Data
public class PublicResponseHeaders {
    private String contentLength;
    private String contentType;
    private String date;
    private String eTag;
    private String lastModified;
    private String server;
    private String transferEncoding;
}