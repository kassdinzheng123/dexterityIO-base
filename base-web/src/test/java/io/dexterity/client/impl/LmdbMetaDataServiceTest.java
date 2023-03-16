package io.dexterity.client.impl;

import io.dexterity.service.impl.LmdbMetaDataService;
import io.dexterity.DexterityIOEntrance;
import io.dexterity.client.MultipleEnv;
import io.dexterity.client.MultipleLmdb;
import io.dexterity.entity.LMDBEnvSettings;
import io.dexterity.entity.LMDBEnvSettingsBuilder;
import io.dexterity.entity.MetaData;
import jakarta.annotation.Resource;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

import java.util.*;

@SpringBootTest(classes = DexterityIOEntrance.class)
@Slf4j
class LmdbMetaDataServiceTest {

    @Resource
    private LmdbMetaDataService lmdbMetaDataService;

    @Test
    void buildEnvBeforeTest(){
        LMDBEnvSettings settings = LMDBEnvSettingsBuilder.startBuild()
                .envName("metadata-test-bucket")
                .maxReaders(100)
                .maxSize(1024 * 1024 * 5L)
                .maxDBInstance(30)
                .filePosition("E:\\Resource\\metadata-test")
                .build();
        MultipleEnv multipleEnv = MultipleLmdb.buildNewEnv(settings);
    }

    @Test
    void insertNewMetadata() {
//        for (int i = 0; i < 10000; i++) {
//            MetaData metaData = new MetaData();
//            metaData.key = UUID.randomUUID().toString();
//            metaData.setCheckSum(UUID.randomUUID().toString());
//            metaData.setMIME(UUID.randomUUID().toString());
//            metaData.setSize(UUID.randomUUID().toString());
//            metaData.setCreateUTC(UUID.randomUUID().toString());
//            metaData.setUpdateUTC(UUID.randomUUID().toString());
//        }

        MetaData metaData = new MetaData();
        metaData.key = UUID.randomUUID().toString();
        metaData.setCheckSum(UUID.randomUUID().toString());
        metaData.setMIME(UUID.randomUUID().toString());
        metaData.setSize(UUID.randomUUID().toString());
        metaData.setCreateUTC(UUID.randomUUID().toString());
        metaData.setUpdateUTC(UUID.randomUUID().toString());
        lmdbMetaDataService.insertNewMetadata(metaData,"metadata-test-bucket");

    }

    @Test
    void insertPatchData() {
        List<MetaData> mdList = new ArrayList<>();
        List<MetaData> query = new ArrayList<>();
        for (int i = 0; i < 10000; i++) {
            MetaData metaData = new MetaData();
            metaData.key = UUID.randomUUID().toString();
            metaData.setCheckSum(UUID.randomUUID().toString());
            metaData.setMIME(UUID.randomUUID().toString());
            metaData.setSize(UUID.randomUUID().toString());
            metaData.setCreateUTC(UUID.randomUUID().toString());
            String updateUTC = UUID.randomUUID().toString();
            metaData.setUpdateUTC(updateUTC);
            query.add(metaData);
            mdList.add(metaData);
        }
        for (int i = 0; i < 10000; i++) {
            MetaData metaData = new MetaData();
            metaData.key = UUID.randomUUID().toString();
            metaData.setCheckSum(UUID.randomUUID().toString());
            metaData.setMIME(UUID.randomUUID().toString());
            metaData.setSize(UUID.randomUUID().toString());
            metaData.setCreateUTC(UUID.randomUUID().toString());
            metaData.setUpdateUTC(UUID.randomUUID().toString());
            mdList.add(metaData);
        }
        long start = System.currentTimeMillis();
        lmdbMetaDataService.insertPatch(mdList,"metadata-test-bucket");
        long end = System.currentTimeMillis()-start;
        log.info("cases last {} ms", end);
        for (var s:query){
            Set<String> strings = lmdbMetaDataService.selectByMetaData(s, "metadata-test-bucket");
        }
    }

    @Test
    void deleteMetadata() {
        List<MetaData> mdList = new ArrayList<>();
        List<MetaData> query = new ArrayList<>();
        for (int i = 0; i < 10000; i++) {
            MetaData metaData = new MetaData();
            metaData.key = UUID.randomUUID().toString();
            metaData.setCheckSum(UUID.randomUUID().toString());
            metaData.setMIME(UUID.randomUUID().toString());
            metaData.setSize(UUID.randomUUID().toString());
            metaData.setCreateUTC(UUID.randomUUID().toString());
            String updateUTC = UUID.randomUUID().toString();
            metaData.setUpdateUTC(updateUTC);
            query.add(metaData);
            mdList.add(metaData);
        }
        for (int i = 0; i < 10000; i++) {
            MetaData metaData = new MetaData();
            metaData.key = UUID.randomUUID().toString();
            metaData.setCheckSum(UUID.randomUUID().toString());
            metaData.setMIME(UUID.randomUUID().toString());
            metaData.setSize(UUID.randomUUID().toString());
            metaData.setCreateUTC(UUID.randomUUID().toString());
            metaData.setUpdateUTC(UUID.randomUUID().toString());
            mdList.add(metaData);
        }
        long start = System.currentTimeMillis();
        lmdbMetaDataService.insertPatch(mdList,"metadata-test-bucket");
        long end = System.currentTimeMillis()-start;

        List<String> delete = new ArrayList<>();
        query.forEach(
                q -> delete.add(q.key)
        );
        lmdbMetaDataService.deleteMetadata(delete,"metadata-test-bucket");
    }

    @Test
    void selectByMetaData() {
        MetaData metaData = new MetaData();
        metaData.setMIME("231312");
        long start = System.currentTimeMillis();
        Set<String> strings = lmdbMetaDataService.selectByMetaData(metaData, "metadata-test-bucket");
        long end = System.currentTimeMillis() - start;
        System.out.println(end);
        System.out.println(strings);
    }

    @Test
    void selectMdByKeyRange(){
        List<MetaData> mdList = new ArrayList<>();
        List<MetaData> query = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            MetaData metaData = new MetaData();
            metaData.key = "prefix-" + UUID.randomUUID();
            metaData.setCheckSum(UUID.randomUUID().toString());
            metaData.setMIME(UUID.randomUUID().toString());
            metaData.setSize(UUID.randomUUID().toString());
            metaData.setCreateUTC(UUID.randomUUID().toString());
            String updateUTC = String.valueOf(1000000+i);
            metaData.setUpdateUTC(updateUTC);
            query.add(metaData);
            mdList.add(metaData);
        }
        for (int i = 0; i < 10; i++) {
            MetaData metaData = new MetaData();
            metaData.key = UUID.randomUUID().toString();
            metaData.setCheckSum(UUID.randomUUID().toString());
            metaData.setMIME(UUID.randomUUID().toString());
            metaData.setSize(UUID.randomUUID().toString());
            metaData.setCreateUTC(UUID.randomUUID().toString());
            metaData.setUpdateUTC(UUID.randomUUID().toString());
            mdList.add(metaData);
        }
        lmdbMetaDataService.insertPatch(mdList,"metadata-test-bucket");

        long start = System.currentTimeMillis();
        Map<String, MetaData> res1 = lmdbMetaDataService.selectMdByKeyRange(null, null, "metadata-test-bucket");
        Map<String, MetaData> res2 = lmdbMetaDataService.selectMdByKeyPrefix("prefix-", "metadata-test-bucket");
        long end = System.currentTimeMillis();
        log.info("Case lasts {}",end-start);
        System.out.println(res1);
        System.out.println(res2);
    }

    @Test
    void selectMdByMdRange() {
        List<MetaData> mdList = new ArrayList<>();
        List<MetaData> query = new ArrayList<>();
        for (int i = 0; i < 10000; i++) {
            MetaData metaData = new MetaData();
            metaData.key = UUID.randomUUID().toString();
            metaData.setCheckSum(UUID.randomUUID().toString());
            metaData.setMIME(UUID.randomUUID().toString());
            metaData.setSize(UUID.randomUUID().toString());
            metaData.setCreateUTC(UUID.randomUUID().toString());
            String updateUTC = String.valueOf(1000000+i);
            metaData.setUpdateUTC(updateUTC);
            query.add(metaData);
            mdList.add(metaData);
        }
        for (int i = 0; i < 100000; i++) {
            MetaData metaData = new MetaData();
            metaData.key = UUID.randomUUID().toString();
            metaData.setCheckSum(UUID.randomUUID().toString());
            metaData.setMIME(UUID.randomUUID().toString());
            metaData.setSize(UUID.randomUUID().toString());
            metaData.setCreateUTC(UUID.randomUUID().toString());
            metaData.setUpdateUTC(UUID.randomUUID().toString());
            mdList.add(metaData);
        }
        lmdbMetaDataService.insertPatch(mdList,"metadata-test-bucket");

        long start = System.currentTimeMillis();
        Map<String, MetaData> updateUTC = lmdbMetaDataService.selectMdByMdRange("updateUTC", "1000000", "1010000", "metadata-test-bucket");
        long end = System.currentTimeMillis();
        log.info("Case lasts {}",end-start);
        System.out.println(updateUTC.size());
    }


//    @BeforeAll
//    public static void envDestroy(){
//        envs.forEach(
//                (key,value)->{
//                    if (!value.getEnv().isClosed()){
//                        try{
//                            value.getEnv().close();
//                            log.info("LMDB destroy : Env {} is closed",key);
//                        }catch (Env.AlreadyClosedException e){
//                            log.warn("LMDB destroy Exception: Env {} is closed twice",key);
//                        }catch (Exception e){
//                            log.info("LMDB destroy Exception: Env {} failed to close,the data might lose",key);
//                        }
//                    }
//                }
//        );
//    }

}