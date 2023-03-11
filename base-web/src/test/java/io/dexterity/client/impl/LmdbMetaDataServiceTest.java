package io.dexterity.client.impl;

import io.dexterity.DexterityIOEntrance;
import io.dexterity.client.MultipleEnv;
import io.dexterity.client.MultipleLmdb;
import io.dexterity.entity.LMDBEnvSettings;
import io.dexterity.entity.LMDBEnvSettingsBuilder;
import io.dexterity.entity.MetaData;
import io.dexterity.service.impl.LmdbMetaDataService;
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
                .maxSize(1024 * 1024 * 5)
                .maxDBInstance(30)
                .filePosition("E:\\Resource\\metadata-test")
                .build();
        MultipleEnv multipleEnv = MultipleLmdb.buildNewEnv(settings);
    }

    @Test
    void insertNewMetadata() {
        long start = System.currentTimeMillis();
        for (int i = 0; i < 10000; i++) {
            MetaData metaData = new MetaData();
            metaData.key = UUID.randomUUID().toString();
            metaData.setCheckSum(UUID.randomUUID().toString());
            metaData.setMIME(UUID.randomUUID().toString());
            metaData.setSize(UUID.randomUUID().toString());
            metaData.setCreateUTC(UUID.randomUUID().toString());
            metaData.setUpdateUTC(UUID.randomUUID().toString());
            lmdbMetaDataService.insertNewMetadata(metaData,"metadata-test-bucket");
        }
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

        lmdbMetaDataService.insertPatch(mdList,"metadata-test-bucket");
//
        long start = System.currentTimeMillis();
        for (var s:query){
            Set<String> strings = lmdbMetaDataService.selectByMetaData(s, "metadata-test-bucket");
        }
        long end = System.currentTimeMillis()-start;
//
        log.info("cases last {} ms", end);
//
//        List<String> keys = new ArrayList<>();
//        for (var s:query){
//            keys.add(s.key);
//        }
//
//        Map<String, MetaData> stringMetaDataMap = lmdbMetaDataService.selectMetadata(keys, "metadata-test-bucket");


    }

    @Test
    void deleteMetadata() {

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
    void addNewMetadata() {

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