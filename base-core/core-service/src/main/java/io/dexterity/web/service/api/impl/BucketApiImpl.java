package io.dexterity.web.service.api.impl;

import io.dexterity.bucket.po.pojo.Bucket;
import io.dexterity.bucket.service.BucketService;
import io.dexterity.web.service.api.BucketApi;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
public class BucketApiImpl implements BucketApi {
    @Autowired
    private BucketService bucketService;

    @Override
    public int createBucket(Bucket bucket) {
        return bucketService.createBucket(bucket);
    }

    @Override
    public int deleteBucket() {
        return bucketService.deleteBucket();
    }

    @Override
    public List<Bucket> listBucket() {
        return bucketService.listBucket();
    }
}
