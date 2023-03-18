package io.dexterity.po.vo;

import cn.hutool.core.date.DateUtil;
import cn.hutool.json.JSONArray;
import com.baomidou.mybatisplus.annotation.TableId;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class BucketVO {
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
    private Integer status = 1;
    /**
     * 创建时间
     */
    private String createTime = DateUtil.today();
    /**
     * 存储桶标签,KV
     */
    private JSONArray tags = new JSONArray();
}
