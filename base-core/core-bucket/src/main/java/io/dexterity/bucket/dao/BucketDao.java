package io.dexterity.bucket.dao;

import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import io.dexterity.bucket.po.pojo.Bucket;
import org.apache.ibatis.annotations.Mapper;

@Mapper
public interface BucketDao extends BaseMapper<Bucket> {
}
