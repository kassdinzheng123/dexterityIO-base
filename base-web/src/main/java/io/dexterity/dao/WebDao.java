package io.dexterity.dao;

import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import io.dexterity.po.vo.ChunkVO;
import org.apache.ibatis.annotations.Mapper;

@Mapper
public interface WebDao extends BaseMapper<ChunkVO> {

}
