package io.dexterity.client;

import lombok.Getter;
import org.lmdbjava.Dbi;
import org.lmdbjava.DbiFlags;
import org.lmdbjava.Env;

import java.nio.ByteBuffer;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author haoran
 * LMDB 数据库客户端
 */

public class MultipleEnv {

    public MultipleEnv(String envName, Env<ByteBuffer> env){
        this.env = env;
        this.envName = envName;
    }

    @Getter
    private final String envName;

    @Getter
    private final Env<ByteBuffer> env;
    @Getter
    private final ConcurrentHashMap<String, MultipleDBi> lmdbMaps = new ConcurrentHashMap<>();

    /**
     * 生成DB实例
     * @param dbName DBName
     * @param isFixDuplicated 是否采用重复键 且保证重复键的值大小相同
     * @param isSortedDuplicated 是否采用重复键 但重复键的值大小可不同
     * @return 一个LMDBClient实例
     * @throws LMDBCreateFailedException LMDB数据库创建失败
     */
    public MultipleDBi buildDBInstance(String dbName,
                                       boolean isFixDuplicated,
                                       boolean isSortedDuplicated)
            throws LMDBCreateFailedException {
        // 创建数据库实例
        if (lmdbMaps.get(dbName) != null) return lmdbMaps.get(dbName);
        Dbi<ByteBuffer> db;
        try {
            DbiFlags duplicatedFlag = null;
            if (isFixDuplicated) duplicatedFlag = DbiFlags.MDB_DUPFIXED;
            if (isSortedDuplicated) duplicatedFlag = DbiFlags.MDB_DUPSORT;
            if (isFixDuplicated || isSortedDuplicated){
                db = env.openDbi(dbName,DbiFlags.MDB_CREATE,duplicatedFlag);
            }else{
                db = env.openDbi(dbName,DbiFlags.MDB_CREATE);
            }
            MultipleLmdb.writeDBsInfo(isFixDuplicated,isSortedDuplicated,dbName);
            MultipleDBi multipleDBi = new MultipleDBi(db, envName);
            lmdbMaps.put(dbName, multipleDBi);
            return multipleDBi;
        }catch (Exception e) {
            throw new LMDBCreateFailedException(e);
        }
    }

    public static class LMDBCreateFailedException extends Exception{
        public LMDBCreateFailedException(Exception e){
            super("LMDB DBI Create Failed! Check your parameters and the Env,caused by " + e.getMessage());
        }
    }
}
