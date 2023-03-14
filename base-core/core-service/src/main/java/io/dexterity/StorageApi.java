package io.dexterity;

import io.dexterity.po.vo.RocksDBVo;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;

import java.util.List;
import java.util.Set;

public interface StorageApi {
    int getCount( String cfName) throws RocksDBException;
    List<byte[]> getAllKey(String cfName) throws RocksDBException;
    List<byte[]> getKeysFrom(String cfName,byte[] lastKey) throws RocksDBException;
    List<RocksDBVo> getAll(String cfName) throws RocksDBException;
    RocksDBVo get(String cfName, byte[] key) throws RocksDBException;
    RocksDBVo delete(String cfName, byte[] key) throws RocksDBException;
    int putBatch(List<RocksDBVo> rocksDBVos) throws RocksDBException;
    int put(RocksDBVo rocksDBVo) throws RocksDBException;
    Set<String> cfAll();
    int cfDelete(String cfName) throws RocksDBException;
    int cfAdd(String cfName) throws RocksDBException;
    RocksIterator getIterator(String chunkTmp) throws RocksDBException;
}
