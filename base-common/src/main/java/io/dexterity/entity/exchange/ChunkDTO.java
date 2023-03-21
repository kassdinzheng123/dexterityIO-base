package io.dexterity.entity.exchange;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class ChunkDTO {

    private String crypto;//文件的sha256值
    private String chunkCrypto; //块的sha256值
    private Integer index;//块的序号
    private Integer chunkTotal; //块的总数
    private Long fileSize; //文件大小
    private String fileName;//文件名称
    private Long chunkSize; //每块的大小
    private String bucketName; //存储桶

}
