package io.dexterity.controller;

import io.dexterity.annotation.*;
import io.dexterity.aspect.LmdbTxn;
import io.dexterity.client.MultipleEnv;
import io.dexterity.client.MultipleLmdb;
import io.dexterity.entity.LMDBEnvSettings;
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

@RestController
@RequestMapping("/metadata")
public class MetaDataController {
    @Resource
    private MetaDataService metaDataService;

    /**
     * PUT OBJECT
     * @param object
     * @param metadata
     * @param dupName
     * @param unDupName
     * @param bucketName
     * @return
     */
    @LmdbWrite
    @PostMapping("/object")
    ResponseEntity<?> putObject(
            @RequestParam("chunk") MultipartFile object, @RequestParam("metadata") MetaData metadata,
            @RequestParam("dupName") @DupNames List<String> dupName, @RequestParam("unDupName") @UnDupNames List<String> unDupName,
            @RequestParam("bucketName") @BucketName String bucketName){
        Txn<ByteBuffer> writeTxn = LmdbTxn.getWriteTxn(bucketName);
        MultipleEnv env = LmdbTxn.getEnv(bucketName);
        metaDataService.insertNewMetadata(metadata,env,writeTxn);
        //TODO 保存对象到Storage
        return new ResponseEntity<>(HttpStatus.OK);
    }


    @LmdbWrite
    @PostMapping("/md/{key}/{bucket}")
    ResponseEntity<?> updateMetadata(@PathVariable @BucketName String bucket, @PathVariable String key,
                                     @RequestBody @UnDupNames List<String> unDupName,
                                     @RequestBody Map<String,String> additionalMD){
        MultipleEnv env = LmdbTxn.getEnv(bucket);
        Txn<ByteBuffer> writeTxn = LmdbTxn.getWriteTxn(bucket);
        metaDataService.addNewMetadata(key,env,writeTxn,additionalMD);
        return new ResponseEntity<>(HttpStatus.OK);
    }

    @PostMapping("/open")
    ResponseEntity<?> openBucket(@RequestBody LMDBEnvSettings lmdbEnvSettings){
        MultipleLmdb.buildNewEnv(lmdbEnvSettings);
        return new ResponseEntity<>(HttpStatus.OK);
    }

    @LmdbRead
    @PostMapping("/list")
    ResponseEntity<?> listObjects(@RequestBody RangeQuery rangeQuery,
                     @RequestParam MatcherQuery matcherQuery){
        return new ResponseEntity<>(HttpStatus.OK);
    }

    @LmdbWrite
    @DeleteMapping("/delete/{bucketName}")
    ResponseEntity<?> deleteObjects(@RequestBody List<String> objectIds, @PathVariable @BucketName String bucketName){
        MultipleEnv env = LmdbTxn.getEnv(bucketName);
        Txn<ByteBuffer> writeTxn = LmdbTxn.getWriteTxn(bucketName);
        metaDataService.deleteMetadata(objectIds, env, writeTxn);
        //TODO 删除 RocksDB的数据
        return new ResponseEntity<>(HttpStatus.OK);
    }

    @LmdbRead
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
     * 元数据获取接口
     * @param key key
     * @param bucketName bucketName
     * @return
     */
    @LmdbRead
    @GetMapping("/head/{key}/{bucketName}")
    ResponseEntity<?> getMd(@PathVariable String key, @PathVariable String bucketName){
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

    @DeleteMapping("/{key}/{bucketName}")
    ResponseEntity<?> deleteObject(@PathVariable String key,@PathVariable String bucketName){
        MultipleEnv env = LmdbTxn.getEnv(bucketName);
        Txn<ByteBuffer> writeTxn = LmdbTxn.getWriteTxn(bucketName);
        //TODO metadata依法保留部分
        metaDataService.deleteMetadata(List.of(key), env, writeTxn);
        //TODO 删除 RocksDB的数据
        return new ResponseEntity<>(HttpStatus.OK);
    }


}
