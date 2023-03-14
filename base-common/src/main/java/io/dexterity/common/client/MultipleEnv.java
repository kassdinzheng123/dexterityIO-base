package io.dexterity.common.client;

import com.alicp.jetcache.Cache;
import com.alicp.jetcache.embedded.CaffeineCacheBuilder;
import io.dexterity.common.client.exception.LMDBCommonException;
import lombok.Getter;
import lombok.Setter;
import org.lmdbjava.Dbi;
import org.lmdbjava.DbiFlags;
import org.lmdbjava.Env;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

/**
 * @author haoran
 * LMDB 数据库客户端
 */

public class MultipleEnv {

    public MultipleEnv(String envName, Env<ByteBuffer> env, int cacheSize,int expireMin){
        this.env = env;
        this.envName = envName;
        this.cacheSize = cacheSize;
        this.expireMin = expireMin;
        this.cache = CaffeineCacheBuilder.createCaffeineCacheBuilder()
                .limit(cacheSize)
                .expireAfterWrite(expireMin, TimeUnit.SECONDS)
                .buildCache();
    }

    public MultipleEnv(String envName, Env<ByteBuffer> env){
        this.env = env;
        this.envName = envName;
        this.cache = CaffeineCacheBuilder.createCaffeineCacheBuilder()
                .limit(cacheSize)
                .expireAfterWrite(expireMin, TimeUnit.SECONDS)
                .buildCache();
    }

    @Getter
    private final String envName;

    @Getter
    @Setter
    private String userName;

    @Getter
    @Setter
    private String password = "ABC-CDQ-QDF-AZX-SDF";

    private int expireMin = 360;

    private int cacheSize = 10000;

    Cache<String, List<String>> cache;

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
                                       boolean isSortedDuplicated,
                                       String password)
            throws LMDBCreateFailedException {
        // 创建数据库实例
        return getMultipleDBi(dbName, isFixDuplicated, isSortedDuplicated, password);
    }

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
        return getMultipleDBi(dbName, isFixDuplicated, isSortedDuplicated, password);
    }

    /**
     * 生成DB实例
     * @param dbName DBName
     * @param isFixDuplicated 是否采用重复键 且保证重复键的值大小相同
     * @param isSortedDuplicated 是否采用重复键 但重复键的值大小可不同
     * @return 一个LMDBClient实例
     * @throws LMDBCreateFailedException LMDB数据库创建失败
     */
    private MultipleDBi getMultipleDBi(String dbName, boolean isFixDuplicated, boolean isSortedDuplicated, String password) throws LMDBCreateFailedException {
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
            MultipleLmdb.writeDBsInfo(isFixDuplicated,isSortedDuplicated,dbName,password);
            MultipleDBi multipleDBi = new MultipleDBi(db, envName, password,cache);
            lmdbMaps.put(dbName, multipleDBi);
            return multipleDBi;
        }catch (Exception e) {
            e.printStackTrace();
            throw new LMDBCreateFailedException(e);
        }
    }

    public MultipleDBi getDB(String dbKey){
        return this.lmdbMaps.get(dbKey);
    }


    public static class LMDBCreateFailedException extends Exception{
        public LMDBCreateFailedException(Exception e){
            super("LMDB DBI Create Failed! Check your parameters and the Env,caused by " + e.getMessage());
        }
    }

    public Env<ByteBuffer> getEnv(){
        if (this.env == null) throw new LMDBCommonException("env is not build");
        return this.env;
    }
}
