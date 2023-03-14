package io.dexterity.impl;

import io.dexterity.BucketApi;
import io.dexterity.po.vo.BucketVO;
import io.dexterity.service.BucketService;
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
    public int deleteBucket(String bucketId) {
        return bucketService.deleteBucket(bucketId);
    }

    @Override
    public List<BucketVO> listBucket() {
        return bucketService.listBucket();
    }

    @Override
    public int updateStatusBucket(String bucketId, Integer status) {
        return bucketService.updateStatusBucket(bucketId,status);
    }
}
