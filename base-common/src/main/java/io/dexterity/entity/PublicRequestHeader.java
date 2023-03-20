package io.dexterity.entity;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.http.HttpHeaders;

import java.util.Objects;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class PublicRequestHeader {
    private String authorization;
    private Integer contentLength;
    private String contentType;
    private String contentMD5;
    private String date;
    private String host;
    private String xCosSecurityToken;

    public void readFromHttpHeaders(HttpHeaders headers) {
        this.authorization = headers.getFirst("Authorization");
        this.contentLength = Integer.parseInt(Objects.requireNonNull(headers.getFirst("Content-Length")));
        this.contentType = headers.getFirst("Content-Type");
        this.contentMD5 = headers.getFirst("Content-MD5");
        this.date = headers.getFirst("Date");
        this.host = headers.getFirst("Host");
        this.xCosSecurityToken = headers.getFirst("x-cos-security-token");
    }
}