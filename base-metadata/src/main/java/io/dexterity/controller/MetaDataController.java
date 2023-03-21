package io.dexterity.controller;

import io.dexterity.annotation.*;
import io.dexterity.aspect.LmdbTxn;
import io.dexterity.client.MultipleEnv;
import io.dexterity.entity.BucketInfo;
import io.dexterity.entity.MetaData;
import io.dexterity.entity.exchange.MatcherQuery;
import io.dexterity.entity.exchange.RangeQuery;
import io.dexterity.exception.UnexpectedResultException;
import io.dexterity.service.MetaDataService;
import org.lmdbjava.Txn;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;

import javax.annotation.Resource;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

/**
 * Metadata 服务器对外开放的接口
 */
@RestController
@RequestMapping("/metadata")
public class MetaDataController {
    @Resource
    private MetaDataService metaDataService;

    /**
     * PUT OBJECT
     * @param object 对象的文件
     * @param metadata 元数据对象
     * @param dupName 可能要创建的可重复key数据库名称
     * @param unDupName  可能要创建的不可重复key数据库名称
     * @param bucketName 桶名称
     * @return 返回状态码
     */
    @LmdbTransaction(LmdbTxn.WRITE)
    @PostMapping("/object")
    ResponseEntity<?> putObject(
            @RequestParam("chunk") MultipartFile object, @RequestParam("metadata") MetaData metadata,
            @RequestParam("dupName") @DupNames List<String> dupName, @RequestParam("unDupName") @UnDupNames List<String> unDupName,
            @RequestParam("bucketName") @BucketName String bucketName,Txn<ByteBuffer> writeTxn,MultipleEnv env){
        metaDataService.insertNewMetadata(metadata,env,writeTxn);
        //TODO 保存对象到Storage
        return new ResponseEntity<>(HttpStatus.OK);
    }


    /**
     * 更新或者新增某个对象的元数据
     * @param bucket 桶名称
     * @param key 对象的key
     * @param additionalMD 要新增和替换的元数据 以key-value形式存储在Map中
     * @return 状态码
     */
    @LmdbTransaction(LmdbTxn.WRITE)
    @PostMapping("/md/{key}/{bucket}")
    ResponseEntity<?> updateMetadata(@PathVariable @BucketName String bucket, @PathVariable String key,
                                     @RequestBody @DupNames List<String> unDupName,
                                     @RequestBody Map<String,String> additionalMD){
        MultipleEnv env = LmdbTxn.getEnv(bucket);
        Txn<ByteBuffer> writeTxn = LmdbTxn.getWriteTxn(bucket);
        metaDataService.addNewMetadata(key,env,writeTxn,additionalMD);
        return new ResponseEntity<>(HttpStatus.OK);
    }

    /**
     * 为一个bucket开启一个LMDB环境
     * @param lmdbEnvSettings LMDB环境设置
     * @return
     */
    @PostMapping("/open")
    ResponseEntity<?> openBucket(@RequestBody BucketInfo bucketInfo){


//        MultipleLmdb.buildNewEnv(lmdbEnvSettings);
        //TODO rocksDB开启环境
        return new ResponseEntity<>(HttpStatus.OK);
    }

    /**
     * ListObject 查询一个桶下的数据
     * @param rangeQuery
     * @param matcherQuery
     * @return
     */
    @LmdbTransaction(LmdbTxn.READ)
    @PostMapping("/list")
    ResponseEntity<?> listObjects(@RequestBody RangeQuery rangeQuery,
                     @RequestParam MatcherQuery matcherQuery){
        return new ResponseEntity<>(HttpStatus.OK);
    }

    /**
     * 批量删除数据
     * @param objectIds object的ID
     * @param bucketName bucket名称
     * @return 状态码
     */
    @LmdbWrite
    @DeleteMapping("/delete/{bucketName}")
    ResponseEntity<?> deleteObjects(@RequestBody List<String> objectIds, @PathVariable @BucketName String bucketName){
        MultipleEnv env = LmdbTxn.getEnv(bucketName);
        Txn<ByteBuffer> writeTxn = LmdbTxn.getWriteTxn(bucketName);
        metaDataService.deleteMetadata(objectIds, env, writeTxn);
        //TODO 删除 RocksDB的数据
        return new ResponseEntity<>(HttpStatus.OK);
    }

    /**
     * 删除一条数据
     * @param key object的ID
     * @param bucketName bucket名称
     * @return 状态码
     */
    @DeleteMapping("/{key}/{bucketName}")
    ResponseEntity<?> deleteObject(@PathVariable String key,@PathVariable String bucketName){
        MultipleEnv env = LmdbTxn.getEnv(bucketName);
        Txn<ByteBuffer> writeTxn = LmdbTxn.getWriteTxn(bucketName);
        //TODO metadata依法保留部分
        metaDataService.deleteMetadata(List.of(key), env, writeTxn);
        //TODO 删除 RocksDB的数据
        return new ResponseEntity<>(HttpStatus.OK);
    }

    /**
     * 获取一个对象
     * @param key key
     * @param bucketName bucketName
     * @return 对象的数据（请求头为元数据，请求体为对象字节码）
     */
    @LmdbTransaction(LmdbTxn.READ)
    @GetMapping("/get/{key}/{bucketName}")
    ResponseEntity<?> getObject(@PathVariable String key,@PathVariable @BucketName String bucketName){
        MultipleEnv env = LmdbTxn.getEnv(bucketName);
        Txn<ByteBuffer> readTxn = LmdbTxn.getReadTxn(bucketName);
        Map<String, MetaData> stringMetaDataMap = metaDataService.selectMdByKeys(List.of(key), env, readTxn);
        MetaData metaData = stringMetaDataMap.get(key);

        //TODO 利用metadata获取分块,合并并返回
        return new ResponseEntity<>(HttpStatus.OK);
    }

    /**
     * 获取一个对象的元数据
     * @param key key
     * @param bucketName bucketName
     * @return Metadata类
     */
    @LmdbTransaction(LmdbTxn.READ)
    @GetMapping("/head/{key}/{bucketName}")
    ResponseEntity<MetaData> getMd(@PathVariable String key, @PathVariable String bucketName){
        MultipleEnv env = LmdbTxn.getEnv(bucketName);
        Txn<ByteBuffer> readTxn = LmdbTxn.getReadTxn(bucketName);
        Map<String, MetaData> stringMetaDataMap = metaDataService.selectMdByKeys(List.of(key), env, readTxn);
        HttpHeaders httpHeaders = new HttpHeaders();
        if (stringMetaDataMap.size() > 1) throw new UnexpectedResultException("Multiple Metadata Found,inner exception may happened");
        stringMetaDataMap.forEach(
                (k,v)->{
                    v.metaDataMap.forEach(
                            httpHeaders::add
                    );
                }
        );
        return new ResponseEntity<>(httpHeaders, HttpStatus.OK);
    }






}
