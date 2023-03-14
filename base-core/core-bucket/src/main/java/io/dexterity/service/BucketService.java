package io.dexterity.service;

import com.baomidou.mybatisplus.extension.service.IService;
import io.dexterity.po.pojo.Bucket;
import io.dexterity.po.vo.BucketVO;

import java.util.List;

public interface BucketService extends IService<Bucket> {
    /**
     * 创建存储桶
     */
    int createBucket(BucketVO bucketVO);

    /**
     * 删除存储桶（id）
     */
    int deleteBucket(String bucketId);

    /**
     * 存储桶状态（id）
     */
    int updateStatusBucket(String bucketId,Integer status);

    /**
     * 查询存储桶列表
     */
    List<BucketVO> listBucket();
}
