package io.dexterity.service.api.impl;

import io.dexterity.bucket.po.vo.BucketVO;
import io.dexterity.bucket.service.BucketService;
import io.dexterity.service.api.BucketApi;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class BucketApiImpl implements BucketApi {
    @Autowired
    private BucketService bucketService;

    @Override
    public int createBucket(BucketVO bucketVO) {
        return bucketService.createBucket(bucketVO);
    }

    @Override
    public int deleteBucket(Integer bucketId) {
        return bucketService.deleteBucket(bucketId);
    }

    @Override
    public List<BucketVO> listBucket() {
        return bucketService.listBucket();
    }

    @Override
    public int updateStatusBucket(Integer bucketId, Integer status) {
        return bucketService.updateStatusBucket(bucketId,status);
    }
}
