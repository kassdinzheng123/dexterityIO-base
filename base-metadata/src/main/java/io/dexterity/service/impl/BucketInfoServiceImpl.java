package io.dexterity.service.impl;

import io.dexterity.client.MultipleDBi;
import io.dexterity.client.MultipleEnv;
import io.dexterity.client.MultipleLmdb;
import io.dexterity.entity.BucketInfo;
import io.dexterity.entity.LMDBEnvSettings;
import io.dexterity.entity.LMDBEnvSettingsBuilder;
import io.dexterity.service.BucketInfoService;
import org.lmdbjava.Txn;

import java.nio.ByteBuffer;

public class BucketInfoServiceImpl implements BucketInfoService {

    @Override
    public void insertBucketInfo(BucketInfo bucketInfo) {
        //第一步 buildEnv
        String metadataLimit = bucketInfo.getMetadataLimit();
        String maxReader = bucketInfo.getMaxReader();

        //为这个 bucket 构建专属环境
        LMDBEnvSettings build = LMDBEnvSettingsBuilder.startBuild().maxSize(1024 * 1024 * 100L)
                .envName(bucketInfo.getName())
                .maxDBInstance(200)
                .maxDBInstance(Integer.parseInt(metadataLimit))
                .maxReaders(Integer.parseInt(maxReader)).filePosition("D://").build();
        MultipleLmdb.buildNewEnv(build);

        //插入到mainEnv中
        MultipleEnv mainEnv = MultipleLmdb.envs.get(MultipleLmdb.MAIN_ENV);
        MultipleDBi db = MultipleLmdb.getBucketDB();
        Txn<ByteBuffer> txn = MultipleLmdb.getMainEnv().txnWrite();
        try (txn) {
            db.putJsonObject(bucketInfo.getName(),bucketInfo,txn);
            txn.commit();
        }catch (Exception e){
            txn.abort();
        }
    }

    @Override
    public void deleteBucketInfo(String bucketName) {
        MultipleEnv mainEnv = MultipleLmdb.envs.get(MultipleLmdb.MAIN_ENV);
        MultipleDBi db = MultipleLmdb.getBucketDB();
        db.delete(bucketName);
    }

    @Override
    public boolean isBucketInfoExist(String bucketName) {
        MultipleEnv mainEnv = MultipleLmdb.envs.get(MultipleLmdb.MAIN_ENV);
        MultipleDBi db = MultipleLmdb.getBucketDB();

        Txn<ByteBuffer> txn = MultipleLmdb.getMainEnv().txnRead();
        try (txn) {
            String s = db.get(bucketName, txn);
            return s != null;
        }
    }

    @Override
    public BucketInfo getBucketInfo(String bucketName) {
        MultipleEnv mainEnv = MultipleLmdb.envs.get(MultipleLmdb.MAIN_ENV);
        MultipleDBi db = MultipleLmdb.getBucketDB();

        Txn<ByteBuffer> txn = MultipleLmdb.getMainEnv().txnRead();
        try (txn) {
            return db.getAsObject(bucketName, txn);
        }
    }

}
