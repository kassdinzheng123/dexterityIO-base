package io.dexterity.service.api;

import io.dexterity.bucket.po.vo.BucketVO;

import java.util.List;

public interface BucketApi {
    int createBucket(BucketVO bucketVO);
    int deleteBucket(Integer bucketId);
    List<BucketVO> listBucket();
    int updateStatusBucket(Integer bucketId,Integer status);
}