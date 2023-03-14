package io.dexterity.po.vo;

import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
@Data
@AllArgsConstructor
@NoArgsConstructor
@TableName(value = "chunk_info")
public class ChunkVO {
    @TableId
    Integer index; //块的序号
    Integer chunkTotal; //块的总数
    Long chunkSize; //每块的大小
    String bucketName; //存储桶
}
