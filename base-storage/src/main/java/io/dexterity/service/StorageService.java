package io.dexterity.service;

import io.dexterity.po.vo.RocksDBVo;
import org.rocksdb.RocksDBException;
import org.rocksdb.Transaction;

import java.util.List;
import java.util.Set;

public interface StorageService {

    Integer getCount( String cfName,Transaction transaction) throws RocksDBException;

    List<byte[]> getKeys(String cfName,Transaction transaction) throws RocksDBException;

    List<byte[]> getKeysFrom(String cfName,byte[] lastKey,Transaction transaction) throws RocksDBException;

//    List<RocksDBVo> getAll(String cfName,Transaction transaction) throws RocksDBException;

    RocksDBVo get(String cfName, byte[] key,Transaction transaction) throws RocksDBException;

    RocksDBVo delete(String cfName, byte[] key,Transaction transaction) throws RocksDBException;
//    int putBatch(List<RocksDBVo> rocksDBVos,Transaction transaction) throws RocksDBException;

    Integer put(RocksDBVo rocksDBVo, Transaction transaction) throws RocksDBException;
    Set<String> cfAll(Transaction transaction);

    Integer cfDelete(String cfName,Transaction transaction) throws RocksDBException;

    Integer cfAdd(String cfName,Transaction transaction) throws RocksDBException;

//    RocksIterator getIterator(String cfName,Transaction transaction) throws RocksDBException;
}
