package io.dexterity.po.pojo;

import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
@TableName(value = "bucket")
public class Bucket {
    /**
     * 存储桶id
     */
    @TableId
    private String bucketId;
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
     * 存储桶状态
     */
    private Integer status;
    /**
     * 创建时间
     */
    private String createTime;
    /**
     * 存储桶标签,KV
     */
    private String tags;
}
