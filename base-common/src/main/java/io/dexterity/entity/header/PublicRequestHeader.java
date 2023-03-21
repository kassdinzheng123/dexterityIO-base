package io.dexterity.entity.header;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Slf4j
public class PublicRequestHeader {
    private String authorization;
    private Integer contentLength;
    private String contentType;
    private String contentMD5;
    private String date;
    private String host;
    private String xCosSecurityToken;

}