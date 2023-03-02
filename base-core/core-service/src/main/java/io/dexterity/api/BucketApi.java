package io.dexterity.api;

import io.dexterity.pojo.po.Bucket;

import java.util.List;

public interface BucketApi {
    int createBucket(Bucket bucket);

    int deleteBucket();

    List<Bucket> listBucket();
}