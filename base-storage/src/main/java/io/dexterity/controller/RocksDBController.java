package io.dexterity.controller;

import io.dexterity.po.R;
import io.dexterity.po.vo.RocksDBVo;
import io.dexterity.service.StorageService;
import io.swagger.v3.oas.annotations.Operation;
import org.rocksdb.RocksDBException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.List;
@RestController
@RequestMapping("/storage")
public class RocksDBController {
    @Autowired
    StorageService storageService;
    @Operation(summary = "查询总条数", description = "查询某个列族键值总条数")
    @GetMapping("/count")
    public R<?> getCount(@RequestParam("cfName") String cfName) throws RocksDBException{
        return new R<>(200,"请求成功",storageService.getCount(cfName));
    }
    @Operation(summary = "查询所有Key", description = "查询某个列族的所有Key值")
    @GetMapping("/keys")
    public R<?> getAllKey(String cfName) throws RocksDBException{
        return new R<>(200,"请求成功",storageService.getAllKey(cfName));
    }
    @Operation(summary = "分片查Key", description = "从lastKey开始查（不包括lastKey），查询某个列族的所有Key值")
    @GetMapping("/keysFrom")
    public R<?> getKeysFrom(String cfName,byte[] lastKey) throws RocksDBException{
        return new R<>(200,"请求成功",storageService.getKeysFrom(cfName,lastKey));
    }
    @Operation(summary = "查所有键值", description = "查询某个列族的所有键值")
    @GetMapping("/kvs")
    public R<?> getAll(String cfName) throws RocksDBException{
        return new R<>(200,"请求成功",storageService.getAll(cfName));
    }
    @Operation(summary = "查", description = "根据列族和key，查询键值对")
    @GetMapping("/kv")
    public R<?> get(String cfName, byte[] key) throws RocksDBException{
        return new R<>(200,"请求成功");
    }
    @Operation(summary = "删", description = "根据列族和key，删除键值对")
    @DeleteMapping("/kv")
    public R<?> delete(String cfName, byte[] key) throws RocksDBException{
        return new R<>(200,"请求成功");
    }
    @Operation(summary = "批量增", description = "批量增加键值对，默认列族是default")
    @PutMapping("/kvs")
    public R<?> putBatch(List<RocksDBVo> rocksDBVos) throws RocksDBException{
        return new R<>(200,"请求成功");
    }
    @Operation(summary = "增", description = "增加键值对，默认列族是default")
    @PutMapping("/kv")
    public R<?>put(RocksDBVo rocksDBVo) throws RocksDBException{
        return new R<>(200,"请求成功");
    }
    @Operation(summary = "查询全部列族名", description = "查询全部的列族名")
    @GetMapping("/cf")
    public R<?> cfAll(){
        return new R<>(200,"请求成功");
    }
    @Operation(summary = "删除列族", description = "如果存在，则删除该列族")
    @DeleteMapping("/cf")
    public R<?> cfDelete(String cfName) throws RocksDBException{
        return new R<>(200,"请求成功");
    }
    @Operation(summary = "创建列族", description = "如果不存在，则创建一个新的列族")
    @PostMapping("/cf")
    public R<?> cfAdd(String cfName) throws RocksDBException{
        return new R<>(200,"请求成功");
    }
    @Operation(summary = "获取迭代器", description = "获取一个指定列族的 RocksDB 迭代器")
    @GetMapping("/")
    public R<?> getIterator(String chunkTmp) throws RocksDBException{
        return new R<>(200,"请求成功");
    }
    @Operation(summary = "获取事务对象", description = "获取该rocksdb的事务对象")
    @GetMapping("/")
    public R<?> getTransaction() throws RocksDBException{
        return new R<>(200,"请求成功");
    }
}
