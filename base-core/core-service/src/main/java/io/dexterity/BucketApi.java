package io.dexterity;

import io.dexterity.po.vo.BucketVO;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
public interface BucketApi {
    int createBucket(BucketVO bucketVO);
    int deleteBucket(Integer bucketId);
    List<BucketVO> listBucket();
    int updateStatusBucket(Integer bucketId,Integer status);
}