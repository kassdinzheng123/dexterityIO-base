package io.dexterity.client;

import io.dexterity.annotation.RocksDBTransactional;
import io.dexterity.po.R;
import io.dexterity.po.vo.RocksDBVo;
import org.rocksdb.RocksDBException;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.service.annotation.HttpExchange;

@HttpExchange("/storage")
public interface StorageClient {
    @GetMapping("/count")
    @RocksDBTransactional
    R<?> getCount(@RequestParam("cfName") String cfName) throws RocksDBException;

    @GetMapping("/keys")
    @RocksDBTransactional
    R<?> getAllKey(@RequestParam("cfName") String cfName) throws RocksDBException;

    @GetMapping("/keysFrom")
    @RocksDBTransactional
    R<?> getKeysFrom(@RequestParam("cfName") String cfName, @RequestParam("lastKey") byte[] lastKey) throws RocksDBException;

    @GetMapping("/kv")
    @RocksDBTransactional
    R<?> get(@RequestParam("cfName") String cfName, @RequestParam("key") byte[] key) throws RocksDBException;

    @DeleteMapping("/kv")
    @RocksDBTransactional
    R<?> delete(@RequestParam("cfName") String cfName, @RequestParam("key") byte[] key) throws RocksDBException;

    @PostMapping("/kv")
    @RocksDBTransactional
    R<?> put(@RequestBody RocksDBVo rocksDBVo) throws RocksDBException;

    @GetMapping("/cf")
    @RocksDBTransactional
    R<?> cfQuery();

    @DeleteMapping("/cf")
    @RocksDBTransactional
    R<?> cfDelete(@RequestParam("cfName") String cfName) throws RocksDBException;

    @PostMapping("/cf")
    @RocksDBTransactional
    R<?> cfAdd(@RequestParam("cfName") String cfName) throws RocksDBException;
}
