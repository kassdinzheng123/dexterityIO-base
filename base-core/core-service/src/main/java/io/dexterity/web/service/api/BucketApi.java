package io.dexterity.web.service.api;

import io.dexterity.bucket.po.pojo.Bucket;

import java.util.List;

public interface BucketApi {
    int createBucket(Bucket bucket);

    int deleteBucket();

    List<Bucket> listBucket();
}