package io.dexterity.service.impl;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.util.IdUtil;
import cn.hutool.json.JSONUtil;
import com.baomidou.mybatisplus.core.conditions.update.UpdateWrapper;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import io.dexterity.client.RocksDBClient;
import io.dexterity.dao.BucketDao;
import io.dexterity.exception.MyException;
import io.dexterity.po.pojo.Bucket;
import io.dexterity.po.vo.BucketVO;
import io.dexterity.service.BucketService;
import org.rocksdb.RocksDBException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class BucketServiceImpl extends ServiceImpl<BucketDao, Bucket> implements BucketService {
    @Autowired
    private BucketDao bucketDao;



    @Override
    public int createBucket(BucketVO bucketVO) throws RocksDBException {
        Bucket bucket = new Bucket();
        BeanUtil.copyProperties(bucketVO,bucket);
        bucket.setTags(bucketVO.getTags().toString());
        bucket.setBucketId(IdUtil.objectId());
        if(bucket.getBucketName() == null || bucket.getBucketName().isBlank())
            throw new MyException(500, "存储桶名称不能为空");
        else if (bucket.getAccessAuthority().isBlank()) {
            throw new MyException(500, "访问权限不能为空");
        } else if (bucket.getDomainName().isBlank()) {
            throw new MyException(500, "域名不能为空");
        } else if (bucket.getRegion().isBlank()) {
            throw new MyException(500, "地区不能为空");
        }
        RocksDBClient.cfAddIfNotExist(bucket.getBucketName());
        return bucketDao.insert(bucket);
    }

    @Override
    public int deleteBucket(String bucketId) {
        return bucketDao.deleteById(bucketId);
    }

    @Override
    public int updateStatusBucket(String bucketId,Integer status) {
        UpdateWrapper<Bucket> wrapper = new UpdateWrapper<>();
        if (status==1){
            wrapper.set("status",0).eq("bucket_id",bucketId);
            return bucketDao.update(null,wrapper);
        }else if(status==0){
            wrapper.set("status",1).eq("bucket_id",bucketId);
            return bucketDao.update(null,wrapper);
        }
        return 0;
    }

    @Override
    public List<BucketVO> listBucket() {
//        List<BucketVO> bucketVOS = new ArrayList<>();
//        List<Bucket> buckets = bucketDao.selectList(null);
//        BeanUtil.copyProperties(buckets,bucketVOS);
//        for(int i=0;i<buckets.size();i++){
//            BeanUtil.copyProperties(buckets.get(i),bucketVOS.get(i));
//            bucketVOS.get(i).setTags(JSONUtil.parseArray(buckets.get(i).getTags()));
//        }
        //可以使用Java8的stream和lambda表达式来优化这个循环算法，代码如下：
        //
        //List<BucketVO> bucketVOS = bucketDao.selectList(null).stream()
        // .map(bucket -> { BucketVO bucketVO = new BucketVO();
        // BeanUtil.copyProperties(bucket, bucketVO);
        // bucketVO.setTags(JSONUtil.parseArray(bucket.getTags())); return bucketVO;
        // }).collect(Collectors.toList());
        //
        //这段代码将数据库查询结果流化，然后使用map操作将每个Bucket对象转换成BucketVO对象，
        //并设置tags属性。
        //最后使用collect操作将转换后的BucketVO对象收集到List中。
        //这种写法不仅代码简洁，而且也更易读、易维护。

        //这段代码使用了Java17的局部变量推断特性var，将BucketVO的类型推断为var，使代码更加简洁。
        //同时，使用了Java17的toList()方法来收集转换后的BucketVO对象，相比Java8的collect(Collectors.toList())更加简洁。
        return bucketDao.selectList(null).stream()
                .map(bucket -> { var bucketVO = new BucketVO();
        BeanUtil.copyProperties(bucket, bucketVO);
        bucketVO.setTags(JSONUtil.parseArray(bucket.getTags()));
        return bucketVO; }) .toList();
    }
}
