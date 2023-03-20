package io.dexterity.controller;


import io.dexterity.annotation.RocksDBTransactional;
import io.dexterity.po.R;
import io.dexterity.po.vo.RocksDBVo;
import io.dexterity.service.StorageService;
import io.swagger.v3.oas.annotations.Operation;
import org.rocksdb.RocksDBException;
import org.rocksdb.Transaction;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/storage")
public class RocksDBController {
    @Autowired
    StorageService storageService;

    Transaction transaction = null;

    @Operation(summary = "查询总条数", description = "查询某个列族键值总条数")
    @GetMapping("/count")
    @RocksDBTransactional
    public R<?> getCount(@RequestParam("cfName") String cfName) throws RocksDBException{
        return new R<>(200,storageService.getCount(cfName,transaction));
    }

    @Operation(summary = "查询所有Key", description = "查询某个列族的所有Key值")
    @GetMapping("/keys")
    @RocksDBTransactional
    public R<?> getAllKey(@RequestParam("cfName") String cfName) throws RocksDBException{
        return new R<>(200,storageService.getKeys(cfName,transaction));
    }

    @Operation(summary = "游标查Key", description = "从lastKey开始查（不包括lastKey），查询某个列族的所有Key值")
    @GetMapping("/keysFrom")
    @RocksDBTransactional
    public R<?> getKeysFrom(@RequestParam("cfName") String cfName, @RequestParam("lastKey") byte[] lastKey) throws RocksDBException{
        return new R<>(200,storageService.getKeysFrom(cfName,lastKey,transaction));
    }

    @Operation(summary = "查", description = "根据列族和key，查询键值对")
    @GetMapping("/kv")
    @RocksDBTransactional
    public R<?> get(@RequestParam("cfName") String cfName, @RequestParam("key") byte[] key) throws RocksDBException{
        return new R<>(200,storageService.get(cfName,key,transaction));
    }

    @Operation(summary = "删", description = "根据列族和key，删除键值对")
    @DeleteMapping("/kv")
    @RocksDBTransactional
    public R<?> delete(@RequestParam("cfName") String cfName, @RequestParam("key") byte[] key) throws RocksDBException{
        return new R<>(200,storageService.delete(cfName,key,transaction));
    }

    @Operation(summary = "增", description = "增加键值对，默认列族是default")
    @PutMapping("/kv")
    @RocksDBTransactional
    public R<?> put(@RequestBody RocksDBVo rocksDBVo) throws RocksDBException{
        return new R<>(200,storageService.put(rocksDBVo,transaction));
    }

    @Operation(summary = "查询全部列族名", description = "查询全部的列族名")
    @GetMapping("/cf")
    @RocksDBTransactional
    public R<?> cfAll(){
        return new R<>(200,storageService.cfAll(transaction));
    }

    @Operation(summary = "删除列族", description = "如果存在，则删除该列族")
    @DeleteMapping("/cf")
    @RocksDBTransactional
    public R<?> cfDelete(@RequestParam("cfName") String cfName) throws RocksDBException{
        return new R<>(200,storageService.cfDelete(cfName,transaction));
    }

    @Operation(summary = "创建列族", description = "如果不存在，则创建一个新的列族")
    @PostMapping("/cf")
    @RocksDBTransactional
    public R<?> cfAdd(@RequestParam("cfName") String cfName) throws RocksDBException{
        return new R<>(200,storageService.cfAdd(cfName,transaction));
    }
}
