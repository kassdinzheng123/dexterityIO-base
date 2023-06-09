package io.dexterity.client;

import cn.hutool.core.bean.BeanUtil;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import io.dexterity.config.properties.LmdbConfigProperties;
import io.dexterity.entity.LMDBEnvSettings;
import io.dexterity.entity.LMDBEnvSettingsBuilder;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.lucene.util.RamUsageEstimator;
import org.lmdbjava.Env;
import org.lmdbjava.Txn;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.io.File;
import java.lang.reflect.Type;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author haoran
 * LMDB Env的封装类
 * 每一个存储桶都单独对应一个Env，
 * 我们使用DB对不同存储桶中的元数据类进行拆分
 */
@Slf4j
@Component
public class MultipleLmdb {
    private static final String LMDB_INFO_DB = "lmdb-infos";
    private static final String LMDB_ENVS_KEY = "lmdb-envs";
    public static final String BUCKET_INFOS = "bucket-infos";
    public static final String MAIN_ENV = "mainEnv";

    private static final Gson gson = new Gson();

    public MultipleLmdb(LmdbConfigProperties lmdbConfigProperties){
        this.lmdbConfigProperties = lmdbConfigProperties;
        initMainEnv();
    }

    @Getter
    private static Env<ByteBuffer> mainEnv;

    @Getter
    private static MultipleDBi mainDB;

    @Getter
    private static MultipleDBi bucketDB;

    //保存所有的ENVS
    @Getter
    public static ConcurrentHashMap<String, MultipleEnv> envs = new ConcurrentHashMap<>();

//    //保存所有的LMDBClientCollections
//    @Getter
//    public static ConcurrentHashMap<String,LMDBEnvCollection> envs;

    /**
     * 重启之后，Env的信息都会丢失
     * 我们需要一个独立与Lmdb的方法，保存Env信息
     * @param lmdbEnvSettings lmdbEnvSettings
     */
    private static void writeEnvsInfo(LMDBEnvSettings lmdbEnvSettings){
        Map<String, Object> stringObjectMap = BeanUtil.beanToMap(lmdbEnvSettings);
        Map<String,List<String>> inserts = new HashMap<>();
        stringObjectMap.forEach(
                (key,value)-> inserts.put(lmdbEnvSettings.getEnvName()+"-"+key,Collections.singletonList(value.toString()))
        );

        try(Txn<ByteBuffer> txn = mainEnv.txnWrite()){
            //环境列表插入一个新的环境 这里要线程同步
            synchronized (MultipleLmdb.class){
                String s = mainDB.get(LMDB_ENVS_KEY,txn);
                Type type = new TypeToken<Set<String>>(){}.getType();
                Set<String> list = gson.fromJson(s,type);
                if (list == null) list = new HashSet<>();
                list.add(lmdbEnvSettings.getEnvName());
                String edited = gson.toJson(list);
                inserts.put(LMDB_ENVS_KEY,Collections.singletonList(edited));
            }

            mainDB.putAll(txn, inserts);
        }

    }

    /**
     * 把数据库信息写入到mainDB中
     * @param isFixDuplicated 是否支持Fix重复键
     * @param isSortedDuplicated 是否支持Sorted重复键
     * @param dbName DB名称
     */
    public static void writeDBsInfo(boolean isFixDuplicated,boolean isSortedDuplicated,String dbName,String password){
        if (dbName.equals(LMDB_INFO_DB)) return;
        Map<String,List<String>> inserts = new HashMap<>();

        inserts.put(dbName + "-isFixDuplicated",Collections.singletonList(isFixDuplicated?"1":"0"));
        inserts.put(dbName + "-isSortedDuplicated",Collections.singletonList(isSortedDuplicated?"1":"0"));
        inserts.put(dbName + "-password",Collections.singletonList(password));

        try(Txn<ByteBuffer> txn = mainEnv.txnWrite()){
            mainDB.putAll(txn, inserts);
            txn.commit();
        }
    }

    public static void removeDBsInfo(String dbName, Txn<ByteBuffer> txn){
        if (dbName.equals(LMDB_INFO_DB)) return;
        List<String> inserts = new ArrayList<>();
        inserts.add(dbName + "-isFixDuplicated");
        inserts.add(dbName + "-isSortedDuplicated");
        inserts.add(dbName + "-password");
        mainDB.deletePatch(inserts,txn);
    }

    /**
     * 创建一个新的ENV
     * @param lmdbEnvSettings LMDBEnv的设置类
     * @return 新Env
     */
    public static MultipleEnv buildNewEnv(LMDBEnvSettings lmdbEnvSettings) {
        Env<ByteBuffer> env = Env.create()
                .setMapSize(lmdbEnvSettings.getMaxSize()) // 容量
                .setMaxDbs(lmdbEnvSettings.getMaxDBInstance()) // 数据库实例
                .setMaxReaders(lmdbEnvSettings.getMaxReaders()) // 读事务
                .open(new File(lmdbEnvSettings.getFilePosition()));
        MultipleEnv value = new MultipleEnv(lmdbEnvSettings.getEnvName(), env);
        envs.put(lmdbEnvSettings.getEnvName(), value);
        writeEnvsInfo(lmdbEnvSettings);
        return value;
    }

    public static void deleteEnv(String envName){

        Env<ByteBuffer> env = envs.remove(envName).getEnv();
        env.close();
        try(Txn<ByteBuffer> txn = mainEnv.txnWrite()) {
            Set<Map.Entry<String,String>> inserts = new HashSet<>();
            String s = mainDB.get(LMDB_ENVS_KEY,txn);
            Type type = new TypeToken<List<String>>(){}.getType();
            Set<String> list = gson.fromJson(s,type);
            list.remove(envName);
            String edited = gson.toJson(list);

            mainDB.put(LMDB_ENVS_KEY,edited,txn);
            txn.commit();
        }
    }

    /**
     * 创建主Env，这个Env将保存所有其他存储桶Env的信息
     */

    @Resource
    LmdbConfigProperties lmdbConfigProperties;

    public void initMainEnv(){
        long start = System.currentTimeMillis();
        //初始化 mainEnv
        mainEnv = Env.create()
                .setMapSize(1024L*1024) // 容量为1MB
                .setMaxDbs(25) // 数据库实例
                .setMaxReaders(256) // 读事务
                .open(new File(lmdbConfigProperties.getMainDBRoot()));
        MultipleEnv multipleEnv = new MultipleEnv("mainEnv", mainEnv);
        envs.put("mainEnv", multipleEnv);
        try {
            mainDB = multipleEnv
                    .buildDBInstance(LMDB_INFO_DB, false, false);
            bucketDB = multipleEnv
                    .buildDBInstance(BUCKET_INFOS, false, false);
        }catch (MultipleEnv.LMDBCreateFailedException e) {
            e.printStackTrace();
            log.error("LMDB init failed: lmdb-info db not correctly created");
            System.exit(-1);
        }

        try (Txn<ByteBuffer> txn = mainEnv.txnWrite();Txn<ByteBuffer> read = mainEnv.txnRead()) {
            //根据mainDB的读取到的数据进行写入 规定：S是一个JSON结构的数组
            String s = mainDB.get(LMDB_ENVS_KEY,txn);
            Type type = new TypeToken<Set<String>>(){}.getType();
            Set<String> list = gson.fromJson(s,type);
            //初始化所有Env
            if (list != null){
                for (String envName:list){
                    initEnvFromMainDB(mainDB,envName,txn);
                }
            }else{
                mainDB.put(LMDB_ENVS_KEY,gson.toJson(Collections.emptyList()),txn);
            }
            txn.commit();
        }
        long end = System.currentTimeMillis();
        log.info("Lmdb Metadata Service: init in {} ms",end-start);
    }

    /**
     * 从系统main数据库里获取历史ENV信息
     * @param multipleDBi mainDB的lmdbClient
     * @param envName 当前env的名称
     */
    private static void initEnvFromMainDB(MultipleDBi multipleDBi, String envName,Txn<ByteBuffer> txn){
        Map<String,String> patch = multipleDBi.getPatch(txn,(envName + "-maxSize"),
                        (envName + "-maxReaders"),
                        (envName + "-maxDBInstance"),
                        (envName + "-filePosition"));
        LMDBEnvSettings settings = LMDBEnvSettingsBuilder.startBuild()
                .envName(envName)
                .filePosition(patch.get(envName + "-filePosition"))
                .maxDBInstance(Integer.parseInt(patch.get(envName + "-maxDBInstance")))
                .maxSize(Long.parseLong(patch.get(envName + "-maxSize")))
                .maxReaders(Integer.parseInt(patch.get(envName + "-maxReaders")))
                .build();
        MultipleEnv multipleEnv = buildNewEnv(settings);
        Env<ByteBuffer> env = multipleEnv.getEnv();

        //开始构建DBS
        List<String> collect = env.getDbiNames().stream().map((b) -> {
            return new String(b, StandardCharsets.UTF_8);
        }).toList();
        for (String dbName: collect) {
            initDBFromMainDB(multipleDBi,multipleEnv,dbName,txn);
        }

    }

    /**
     * 从MainDB信息中获取DB
     * @param multipleDBi lmdb客户端
     * @param dbName dbName
     */
    private static void initDBFromMainDB(MultipleDBi multipleDBi, MultipleEnv multipleEnv, String dbName,Txn<ByteBuffer> txn){
        Map<String,String> patch = multipleDBi.getPatch(txn,(dbName + "-isFixDuplicated"),
                (dbName + "-isSortedDuplicated"));
        String isSortedDuplicated = patch.get(dbName + "-isSortedDuplicated");
        String isFixDuplicated = patch.get(dbName + "-isFixDuplicated");
        try {
            multipleEnv.buildDBInstance(dbName, isFixDuplicated.equals("1"), isSortedDuplicated.equals("1"));
        } catch (MultipleEnv.LMDBCreateFailedException e) {
            log.info("Lmdb history DB create failed");
        }
    }

    public static void checkAndExpand(String envName, Object... objects){

        log.info("LMDB: start expand check");

        MultipleEnv multipleEnv = envs.get(envName);
        if (multipleEnv == null) return;
        Env<ByteBuffer> env = multipleEnv.getEnv();

        long current = env.stat().pageSize * env.info().lastPageNumber;
        long r = RamUsageEstimator.sizeOf(objects);
        long expectedSize = env.info().mapSize;
        log.info("LMDB:current size is {}",current);

        while (r*10 + current > expectedSize*0.8){
            expectedSize *= 2;
            log.info("LMDB: purpose is {}",expectedSize);
        }

        expectedSize *= 2;

        env.setMapSize(expectedSize);

        try (Txn<ByteBuffer> txn = mainEnv.txnWrite()) {
            mainDB.put(envName + "-maxSize", String.valueOf(expectedSize),txn);
        }

        log.info("LMDB: Storage Expand Occurs,old capacity is {},new capacity is {}"
                    ,current/(1024*1024),env.info().mapSize/(1024*1024));


    }

    public static void checkAndReduce(String envName){

        log.info("LMDB: start reduce check");

        MultipleEnv multipleEnv = envs.get(envName);
        if (multipleEnv == null) return;
        Env<ByteBuffer> env = multipleEnv.getEnv();


        long current = env.stat().pageSize * env.info().lastPageNumber;
        log.info("LMDB: current size is {}",current);
        env.setMapSize((long) (current*1.5));

        try (Txn<ByteBuffer> txn = mainEnv.txnWrite()) {
            mainDB.put(envName + "-maxSize", String.valueOf((long) (current*1.5)),txn);
        }

        log.info("LMDB: Storage Reduce Occurs,old capacity is {},new capacity is {}",
                env.info().mapSize/(1024*1024),current*1.5/(1024*1024));


    }



}
