package io.dexterity.service.impl;

import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import io.dexterity.dao.BucketDao;
import io.dexterity.pojo.po.Bucket;
import io.dexterity.service.BucketService;
import lombok.AllArgsConstructor;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
@AllArgsConstructor
public class BucketServiceImpl extends ServiceImpl<BucketDao, Bucket> implements BucketService {
    private final BucketDao bucketDao;


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
        return null;
    }
}
