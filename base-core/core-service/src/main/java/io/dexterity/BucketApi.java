package io.dexterity;

import io.dexterity.po.vo.BucketVO;

import java.util.List;

public interface BucketApi {
    int createBucket(BucketVO bucketVO);
    int deleteBucket(String bucketId);
    List<BucketVO> listBucket();
    int updateStatusBucket(String bucketId,Integer status);
}