package io.dexterity.api.impl;

import io.dexterity.api.BucketApi;
import io.dexterity.pojo.po.Bucket;
import io.dexterity.service.BucketService;
import lombok.AllArgsConstructor;

import java.util.List;

@AllArgsConstructor
public class BucketApiImpl implements BucketApi {
    private final BucketService bucketService;

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
