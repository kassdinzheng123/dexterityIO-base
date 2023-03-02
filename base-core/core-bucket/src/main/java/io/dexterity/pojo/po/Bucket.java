package io.dexterity.pojo.po;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.HashMap;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class Bucket {
    /**
     * 存储桶id
     */
    private Integer bucketId;
    /**
     * 存储桶名称
     */
    private String bucketName;
    /**
     * 访问权限
     */
    private String accessAuthority;
    /**
     * 请求域名
     */
    private String domainName;
    /**
     * 所属地域
     */
    private String region;
    /**
     * 存储桶标签,KV
     */
    private HashMap<String,String> tags;

}
