package io.dexterity.client;

import com.alicp.jetcache.Cache;
import com.alicp.jetcache.embedded.CaffeineCacheBuilder;
import io.dexterity.exception.LMDBCommonException;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.lmdbjava.Dbi;
import org.lmdbjava.DbiFlags;
import org.lmdbjava.Env;
import org.lmdbjava.Txn;

import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

/**
 * @author haoran
 * LMDB 数据库客户端
 */

@Slf4j
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



    public void drop(String dbName,Txn<ByteBuffer> txn){
        MultipleDBi multipleDBi = lmdbMaps.get(dbName);
        multipleDBi.db.drop(txn,true);
        lmdbMaps.remove(dbName);
        log.info("DB {} is dropped",dbName);
    }

    public void dropAll(){
        List<byte[]> dbiNames = env.getDbiNames();
        try(Txn<ByteBuffer> subTxn = env.txnWrite()){
            dbiNames.forEach(
                    bytes -> {
                        ByteBuffer buffer = ByteBuffer.wrap(bytes);
                        Charset charset = StandardCharsets.UTF_8;
                        CharBuffer charBuffer = charset.decode(buffer);
                        MultipleDBi multipleDBi = lmdbMaps.get(charBuffer.toString());
                        multipleDBi.db.drop(subTxn,true);
                        log.info("DB {} is dropped",charBuffer.toString());
                        MultipleLmdb.removeDBsInfo(charBuffer.toString(),subTxn);
                        lmdbMaps.remove(charBuffer.toString());
                    }
            );
            subTxn.commit();
        }
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
        Dbi<ByteBuffer> db;
        try {
            DbiFlags duplicatedFlag = null;
            if (isFixDuplicated) duplicatedFlag = DbiFlags.MDB_DUPFIXED;
            if (isSortedDuplicated) duplicatedFlag = DbiFlags.MDB_DUPSORT;
            if (isFixDuplicated || isSortedDuplicated){
                db = env.openDbi(dbName,DbiFlags.MDB_CREATE,DbiFlags.MDB_DUPSORT);
                Txn<ByteBuffer> txn = env.txnRead();
                log.info("db flags are {}",db.listFlags(txn));
                txn.close();
            }else{
                db = env.openDbi(dbName,DbiFlags.MDB_CREATE);
            }
            MultipleLmdb.writeDBsInfo(isFixDuplicated,isSortedDuplicated,dbName,password);
            MultipleDBi multipleDBi = new MultipleDBi(db, envName, password,cache,isFixDuplicated||isSortedDuplicated);
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

    /**
     * 再使用任何元数据服务之前 ，需要在**打开最外层LMDB事务之前**去进行该操作！
     * @param unDupNames 无重复键的DBName
     * @param dupNames 有重复键的DBName
     */
    public void initDBs(List<String> unDupNames,List<String> dupNames){
        for (String dbName : unDupNames) {
            try {
                buildDBInstance(dbName,false,false);
            } catch (MultipleEnv.LMDBCreateFailedException e) {
                throw new RuntimeException(e);
            }
        }

        for (String dbName : dupNames) {
            try {
                buildDBInstance(dbName,false,true);
            } catch (MultipleEnv.LMDBCreateFailedException e) {
                throw new RuntimeException(e);
            }
        }

    }

    /**
     * 再使用任何元数据服务之前 ，需要在**打开最外层LMDB事务之前**去进行该操作！
     * @param unDupNames 无重复键的DBName
     * @param dupNames 有重复键的DBName
     */
    public Txn<ByteBuffer> writeTxn(List<String> unDupNames,List<String> dupNames){
        initDBs(unDupNames,dupNames);
        return this.getEnv().txnWrite();
    }

    public Txn<ByteBuffer> readTxn(){
        return this.getEnv().txnRead();
    }

    public void rollback(Txn<ByteBuffer> txn){
        txn.abort();
        txn.close();
    }

    public void rollbackWithFullCheck(Txn<ByteBuffer> txn,Object... args){
        txn.abort();
        txn.close();
        MultipleLmdb.checkAndExpand(this.envName,args);
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
