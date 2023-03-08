package io.dexterity.storage.service;

import io.dexterity.storage.po.vo.RocksDBVo;
import io.swagger.v3.oas.annotations.Operation;
import org.rocksdb.RocksDBException;

import java.util.List;
import java.util.Set;

public interface StorageService {
    @Operation(summary = "查询总条数", description = "查询某个列族键值总条数")
    int getCount( String cfName) throws RocksDBException;
    @Operation(summary = "查询所有Key", description = "查询某个列族的所有Key值")
    List<String> getAllKey(String cfName) throws RocksDBException;
    @Operation(summary = "分片查Key", description = "从lastKey开始查（不包括lastKey），查询某个列族的所有Key值")
    List<String> getKeysFrom(String cfName,String lastKey) throws RocksDBException;
    @Operation(summary = "查所有键值", description = "查询某个列族的所有键值")
    List<RocksDBVo> getAll(String cfName) throws RocksDBException;
    @Operation(summary = "批量查", description = "根据列族和key，批量查询键值对")
    List<RocksDBVo> multiGetAsList(List<RocksDBVo> rocksDBVos) throws RocksDBException;
    @Operation(summary = "查", description = "根据列族和key，查询键值对")
    RocksDBVo get(String cfName, String key) throws RocksDBException;
    @Operation(summary = "删", description = "根据列族和key，删除键值对")
    RocksDBVo delete(String cfName, String key) throws RocksDBException;
    @Operation(summary = "批量增", description = "批量增加键值对，默认列族是default")
    int putBatch(List<RocksDBVo> rocksDBVos) throws RocksDBException;
    @Operation(summary = "增", description = "增加键值对，默认列族是default")
    int put(RocksDBVo rocksDBVo) throws RocksDBException;
    @Operation(summary = "查询全部列族名", description = "查询全部的列族名")
    Set<String> cfAll();
    @Operation(summary = "删除列族", description = "如果存在，则删除该列族")
    int cfDelete(String cfName) throws RocksDBException;
    @Operation(summary = "创建列族", description = "如果不存在，则创建一个新的列族")
    int cfAdd(String cfName) throws RocksDBException;
}
