package io.dexterity;

import io.dexterity.po.vo.BucketVO;
import org.rocksdb.RocksDBException;

import java.util.List;

public interface BucketApi {
    int createBucket(BucketVO bucketVO) throws RocksDBException;
    int deleteBucket(String bucketId);
    List<BucketVO> listBucket();
    int updateStatusBucket(String bucketId,Integer status);
}