package io.dexterity.service;

import com.baomidou.mybatisplus.extension.service.IService;
import io.dexterity.pojo.po.Bucket;

import java.util.List;

public interface BucketService extends IService<Bucket> {
    int createBucket(Bucket bucket);

    int deleteBucket();

    List<Bucket> listBucket();
}
