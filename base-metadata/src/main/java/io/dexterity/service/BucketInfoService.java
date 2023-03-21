package io.dexterity.service;

import io.dexterity.entity.BucketInfo;

public interface BucketInfoService {

    void insertBucketInfo(BucketInfo bucketInfo);

    void deleteBucketInfo(String bucketName);

    boolean isBucketInfoExist(String bucketName);

    BucketInfo getBucketInfo(String bucketName);

}
