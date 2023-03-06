package io.dexterity.bucket.service.impl;

import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import io.dexterity.bucket.dao.BucketDao;
import io.dexterity.bucket.po.pojo.Bucket;
import io.dexterity.bucket.service.BucketService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class BucketServiceImpl extends ServiceImpl<BucketDao, Bucket> implements BucketService {
    @Autowired
    private BucketDao bucketDao;

    @Override
    public int createBucket(Bucket bucket) {
        return 0;
    }

    @Override
    public int deleteBucket() {
        return 0;
    }

    @Override
    public List<Bucket> listBucket() {
        return bucketDao.selectList(null);
    }
}
