package io.dexterity.client.impl;

import io.dexterity.DexterityIOEntrance;
import io.dexterity.client.MultipleEnv;
import io.dexterity.client.MultipleLmdb;
import io.dexterity.entity.LMDBEnvSettings;
import io.dexterity.entity.LMDBEnvSettingsBuilder;
import io.dexterity.entity.MetaData;
import io.dexterity.entity.constants.MetaDataConstants;
import io.dexterity.service.impl.TestBean;
import io.dexterity.util.StringFormatUtil;
import jakarta.annotation.Resource;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

import java.util.*;
import java.util.concurrent.*;

@SpringBootTest(classes = DexterityIOEntrance.class)
@Slf4j
class LmdbMetaDataServiceTest {

    @Resource
    private TestBean testBean;

    static int all = 100;
    static int current = 0;

    ExecutorService executor = Executors.newFixedThreadPool(10);

    @Test
    public void build(){
        LMDBEnvSettings build = LMDBEnvSettingsBuilder.startBuild()
                .envName("metadata-test-bucket")
                .filePosition("D:\\Resource\\metadata-test")
                .maxReaders(100)
                .maxDBInstance(100)
                .maxSize(1024 * 1024 * 10L)
                .build();
        MultipleLmdb.buildNewEnv(build);
    }


    @Test
    public void dropAll(){
        MultipleEnv multipleEnv = MultipleLmdb.envs.get("metadata-test-bucket");
        multipleEnv.dropAll();
    }

    @Test
    public void deleteEmpty(){
        current++;
        log.info("Metadata-Service-Unit-Test {}/{} , {} Test Starts",current,all,"Empty Delete");

        MetaData metaData = new MetaData();
        metaData.key = "test-medata-key";
        metaData.setCheckSum(UUID.randomUUID().toString());
        metaData.setMIME(UUID.randomUUID().toString());
        String key = UUID.randomUUID().toString();
        metaData.metaDataMap.put(key,UUID.randomUUID().toString());
        testBean.emptyDelete("metadata-test-bucket",List.of(MetaDataConstants.LMDB_METADATA_KEY),List.of("checkSum","mime",key),metaData);

        log.info("Metadata-Service-Unit-Test {}/{} , {} Test Success",current,all,"Empty Delete");

    }

    @Test
    public void insertSingleAndDelete() throws ExecutionException, InterruptedException {

        current++;
        log.info("Metadata-Service-Unit-Test {}/{} , {} Test Starts",current,all,"Single Insert-Select");

        for (int i = 0; i < 10; i++) {
            Future<String> submit = executor.submit(() -> {
                MetaData metaData = new MetaData();
                metaData.key = "test-medata-key";
                metaData.setCheckSum(UUID.randomUUID().toString());
                metaData.setMIME(UUID.randomUUID().toString());
                String key = UUID.randomUUID().toString();
                metaData.metaDataMap.put(key, UUID.randomUUID().toString());
                testBean.singleInsert("metadata-test-bucket", List.of("checkSum", "mime", key), List.of(MetaDataConstants.LMDB_METADATA_KEY), metaData);
                Map<String, MetaData> stringMetaDataMap = testBean.singleSelect("metadata-test-bucket", List.of(), List.of(), metaData);
                assert stringMetaDataMap.get("test-medata-key") != null;
                return "finished";
            });
        }

        log.info("Metadata-Service-Unit-Test {}/{} , {} Test Success",current,all,"Single Insert-Select");
    }

    @Test
    public void deleteSingle() throws ExecutionException, InterruptedException {
        current++;
        log.info("Metadata-Service-Unit-Test {}/{} , {} Test Starts",current,all,"Single Delete-Select");

        for (int i = 0; i < 10; i++) {
            Future<?> submit = executor.submit(() -> {
                MetaData metaData = new MetaData();
                metaData.key = "test-medata-key";
                metaData.setCheckSum(UUID.randomUUID().toString());
                metaData.setMIME(UUID.randomUUID().toString());
                String key = UUID.randomUUID().toString();
                metaData.metaDataMap.put(key, UUID.randomUUID().toString());
                testBean.deleteNewDataMe("metadata-test-bucket", List.of("checkSum", "mime", key), List.of(), metaData);
                Map<String, MetaData> stringMetaDataMap = testBean.singleSelect("metadata-test-bucket", List.of(), List.of(), metaData);
                assert stringMetaDataMap.get("test-meda-key") == null;
            });
            submit.get();
        }

        log.info("Metadata-Service-Unit-Test {}/{} , {} Test Success",current,all,"Single Delete-Select");
    }

    @Test
    public void insertPatch() throws InterruptedException, ExecutionException {
        current++;
        log.info("Metadata-Service-Unit-Test {}/{} , {} Test Starts",current,all,"Patch Insert-Select-1");

        for (int q = 0; q < 10; q++) {
            Future<?> submit = executor.submit(
                    () -> {
                        List<String> keys = new ArrayList<>();
                        for (int i = 0; i < 100; i++) {
                            MetaData metaData = new MetaData();
                            metaData.key = "test-medata-key-" + i;
                            metaData.setCheckSum("@3");
                            metaData.setMIME("12312");
                            String key = "test-metadata";
                            metaData.metaDataMap.put(key, "13123");
                            testBean.singleInsert("metadata-test-bucket", List.of("checkSum", "mime", key), List.of(), metaData);
                        }

                        Map<String, MetaData> stringMetaDataMap = testBean.patchSelect("metadata-test-bucket", List.of(), List.of(), keys);
                        for (Map.Entry<String, MetaData> stringMetaDataEntry : stringMetaDataMap.entrySet()) {
                            assert stringMetaDataEntry.getKey().contains("test-medata-key-");
                        }
                    }
            );
            submit.get();
        }


        log.info("Metadata-Service-Unit-Test {}/{} , {} Test Success",current,all,"Patch Insert-Select-1");

    }

    @Test
    public void insertPatch2(){
        current++;

        log.info("Metadata-Service-Unit-Test {}/{} , {} Test Starts",current,all,"Patch Insert-Select-2");
        String key = "test-metadata";

        List<MetaData> metaDataList = new ArrayList<>();
        List<String> keys = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            MetaData metaData = new MetaData();
            metaData.key = "test-medata-key-patch-"+i;
            metaData.setCheckSum("@3");
            metaData.setMIME("12312");
            metaData.metaDataMap.put(key,"13123");
            metaDataList.add(metaData);
        }

        testBean.patchInsert("metadata-test-bucket",List.of("checkSum","mime",key),List.of(),metaDataList);
        Map<String, MetaData> stringMetaDataMap = testBean.patchSelect("metadata-test-bucket", List.of(), List.of(), keys);
        for (Map.Entry<String, MetaData> stringMetaDataEntry : stringMetaDataMap.entrySet()) {
            assert stringMetaDataEntry.getKey().contains("test-medata-key-patch");
        }

        log.info("Metadata-Service-Unit-Test {}/{} , {} Test Success",current,all,"Patch Insert-Select-2");
    }

    @Test
    public void addNewMetadata() throws ExecutionException, InterruptedException {
        current++;
        log.info("Metadata-Service-Unit-Test {}/{} , {} Test Starts",current,all,"Patch Insert-PrefixSelect");

        for (int q = 0; q < 10; q++) {
            Future<?> submit = executor.submit(() -> {
                String key = "test-metadata";

                List<MetaData> metaDataList = new ArrayList<>();
                List<String> keys = new ArrayList<>();
                for (int i = 0; i < 10; i++) {
                    MetaData metaData = new MetaData();
                    metaData.key = "test-newData-add" + i;
                    keys.add(metaData.key);
                    metaData.setCheckSum("@3");
                    metaData.setMIME("12312");
                    metaData.metaDataMap.put(key, "13123");
                    metaDataList.add(metaData);
                }

                MetaData metaData = new MetaData();
                keys.add(metaData.key);
                metaData.setCheckSum("@3");
                metaData.setMIME("12312");
                metaData.metaDataMap.put(key, "13123");

                testBean.addNewDetail("metadata-test-bucket", List.of("checkSum", "mime", key,"123"), List.of(),metaData);
                Set<String> strings = testBean.patchSelectByMd("metadata-test-bucket", List.of(), List.of(), metaData);


                Map<String, MetaData> stringMetaDataMap = testBean.patchSelect("metadata-test-bucket", List.of(), List.of(), new ArrayList<>(strings));

                for (MetaData value : stringMetaDataMap.values()) {
                    assert value.metaDataMap.get("123").equals("123");
                }
            });
            submit.get();
        }

        log.info("Metadata-Service-Unit-Test {}/{} , {} Test Success",current,all,"Patch Insert-PrefixSelect");

    }


    @Test
    public void prefixSearch() throws ExecutionException, InterruptedException {
        current++;
        log.info("Metadata-Service-Unit-Test {}/{} , {} Test Starts",current,all,"Patch Insert-PrefixSelect");

        for (int q = 0; q < 10; q++) {
            Future<?> submit = executor.submit(() -> {
                String key = "test-metadata";

                List<MetaData> metaDataList = new ArrayList<>();
                List<String> keys = new ArrayList<>();
                for (int i = 0; i < 10; i++) {
                    MetaData metaData = new MetaData();
                    metaData.key = "test-prefix-search-" + i;
                    keys.add(metaData.key);
                    metaData.setCheckSum("@3");
                    metaData.setMIME("12312");
                    metaData.metaDataMap.put(key, "13123");
                    metaDataList.add(metaData);
                }

                for (int i = 0; i < 10; i++) {
                    MetaData metaData = new MetaData();
                    metaData.key = "test-not-prefix-search-" + i;
                    metaData.setCheckSum("@3");
                    metaData.setMIME("12312");
                    metaData.metaDataMap.put(key, "13123");
                    metaDataList.add(metaData);
                }

                testBean.patchInsert("metadata-test-bucket", List.of("checkSum", "mime", key), List.of(), metaDataList);
                Map<String, MetaData> exact = testBean.patchSelect("metadata-test-bucket", List.of(), List.of(), keys);
                Map<String, MetaData> prefix = testBean.prefixSelect("metadata-test-bucket", List.of(), List.of(), "test-prefix-search-");

                log.info(exact.toString());
                log.info(prefix.toString());
                assert exact.size() == prefix.size();
            });
            submit.get();
        }

        log.info("Metadata-Service-Unit-Test {}/{} , {} Test Success",current,all,"Patch Insert-PrefixSelect");

    }

    @Test
    public void selectByMetaData() throws ExecutionException, InterruptedException {
        current++;

        for (int q = 0; q < 10; q++) {
            Future<?> submit = executor.submit(() -> {
                log.info("Metadata-Service-Unit-Test {}/{} , {} Test Starts", current, all, "Patch Insert-SelectByMD");
                String key = "test-metadata";

                List<MetaData> metaDataList = new ArrayList<>();
                List<String> keys = new ArrayList<>();
                for (int i = 0; i < 10; i++) {
                    MetaData metaData = new MetaData();
                    metaData.key = "test-medata-search-" + i;
                    metaData.setCheckSum(UUID.randomUUID().toString());
                    metaData.setMIME(UUID.randomUUID().toString());
                    metaData.metaDataMap.put(key, UUID.randomUUID().toString());
                    metaDataList.add(metaData);
                }

                for (int i = 0; i < 10; i++) {
                    MetaData metaData = new MetaData();
                    metaData.key = "test-medata-search-purpose-" + i;
                    keys.add(metaData.key);
                    metaData.setCheckSum("checkSum");
                    metaData.setMIME("mime");
                    metaData.metaDataMap.put(key, "myKey");
                    metaDataList.add(metaData);
                }

                MetaData metaData = new MetaData();
                metaData.key = "example";
                metaData.setCheckSum("checkSum");
                metaData.setMIME("mime");
                metaData.metaDataMap.put(key, "myKey");

                testBean.patchInsert("metadata-test-bucket", List.of("checkSum", "mime", key), List.of(MetaDataConstants.LMDB_METADATA_KEY), metaDataList);
                Set<String> strings = testBean.patchSelectByMd("metadata-test-bucket", List.of(), List.of(), metaData);
                Set<String> keySet = new HashSet<>(keys);

                assert strings.size() == keySet.size();
            });
            submit.get();
        }

        log.info("Metadata-Service-Unit-Test {}/{} , {} Test Success",current,all,"Patch Insert-SelectByMD");
    }


    @Test
    public void selectMdByKeyRange() throws ExecutionException, InterruptedException {

        current++;

        for (int q = 0; q < 10; q++) {
            Future<?> submit = executor.submit(() -> {
                log.info("Metadata-Service-Unit-Test {}/{} , {} Test Starts", current, all, "Patch Insert-SelectByRange-Empty");
                String key = "test-metadata";

                List<MetaData> metaDataList = new ArrayList<>();
                for (int i = 0; i < 100; i++) {
                    MetaData metaData = new MetaData();
                    metaData.key = "test-medata-range-extra" + StringFormatUtil.fillInteger(i, 4);
                    metaData.setCheckSum(UUID.randomUUID().toString());
                    metaData.setMIME(UUID.randomUUID().toString());
                    metaData.metaDataMap.put(key, UUID.randomUUID().toString());
                    metaDataList.add(metaData);
                }

                testBean.patchInsert("metadata-test-bucket", List.of("checkSum", "mime", key), List.of(MetaDataConstants.LMDB_METADATA_KEY), metaDataList);
                Map<String, MetaData> stringMetaDataMap = testBean.rangeSelect("metadata-test-bucket", List.of(), List.of(), "test-medata-range-extra0010", "test-medata-range-extra0040", "test-medata-range-extra");
                System.out.println(stringMetaDataMap.size());
                System.out.println(stringMetaDataMap.keySet().stream().sorted().toList());
                assert stringMetaDataMap.size() == 31;
            });
            submit.get();
        }

        log.info("Metadata-Service-Unit-Test {}/{} , {} Test Success",current,all,"Patch Insert-SelectByRange-Empty");
    }

    @Test
    public void selectMdByKeyRange2(){

        current++;

        log.info("Metadata-Service-Unit-Test {}/{} , {} Test Starts",current,all,"Patch Insert-SelectByRange-GreaterThan");
        String key = "test-metadata";

        List<MetaData> metaDataList = new ArrayList<>();
        List<String> keys = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            MetaData metaData = new MetaData();
            metaData.key = "test-medata-range-extra"+ StringFormatUtil.fillInteger(i,4);
            metaData.setCheckSum(UUID.randomUUID().toString());
            metaData.setMIME(UUID.randomUUID().toString());
            metaData.metaDataMap.put(key,UUID.randomUUID().toString());
            metaDataList.add(metaData);
        }


        testBean.patchInsert("metadata-test-bucket",List.of("checkSum","mime",key),List.of(MetaDataConstants.LMDB_METADATA_KEY),metaDataList);
        Map<String, MetaData> stringMetaDataMap = testBean.rangeSelect("metadata-test-bucket", List.of(), List.of(), "test-medata-range-extra0010", null,"test-medata-range-extra");
        System.out.println(stringMetaDataMap.size());
        System.out.println(stringMetaDataMap.keySet().stream().sorted().toList());
        assert stringMetaDataMap.size() == 90;

        log.info("Metadata-Service-Unit-Test {}/{} , {} Test Success",current,all,"Patch Insert-SelectByRange-GreaterThan");
    }

    @Test
    public void selectRangeEmpty(){

        current++;

        log.info("Metadata-Service-Unit-Test {}/{} , {} Test Starts",current,all,"Patch Insert-SelectByRange-GreaterThan");
        String key = "test-metadata";

        List<MetaData> metaDataList = new ArrayList<>();
        List<String> keys = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            MetaData metaData = new MetaData();
            metaData.key = "test-medata-range-extra"+ StringFormatUtil.fillInteger(i,4);
            metaData.setCheckSum(UUID.randomUUID().toString());
            metaData.setMIME(UUID.randomUUID().toString());
            metaData.metaDataMap.put(key,UUID.randomUUID().toString());
            metaDataList.add(metaData);
        }


        testBean.patchInsert("metadata-test-bucket",List.of("checkSum","mime",key),List.of(MetaDataConstants.LMDB_METADATA_KEY),metaDataList);
        Map<String, MetaData> stringMetaDataMap = testBean.rangeSelect("metadata-test-bucket", List.of(), List.of(), "test-medata-range-extra9910", null,"test-medata-range-extra");
        System.out.println(stringMetaDataMap.size());
        System.out.println(stringMetaDataMap.keySet().stream().sorted().toList());
        assert stringMetaDataMap.size() == 90;

        log.info("Metadata-Service-Unit-Test {}/{} , {} Test Success",current,all,"Patch Insert-SelectByRange-GreaterThan");
    }

    @Test
    public void selectMdByKeyRange3(){

        current++;

        log.info("Metadata-Service-Unit-Test {}/{} , {} Test Starts",current,all,"Patch Insert-SelectByRange-LessThan");
        String key = "test-metadata";

        List<MetaData> metaDataList = new ArrayList<>();
        List<String> keys = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            MetaData metaData = new MetaData();
            metaData.key = "test-medata-range-extra"+ StringFormatUtil.fillInteger(i,4);
            metaData.setCheckSum(UUID.randomUUID().toString());
            metaData.setMIME(UUID.randomUUID().toString());
            metaData.metaDataMap.put(key,UUID.randomUUID().toString());
            metaDataList.add(metaData);
        }


        testBean.patchInsert("metadata-test-bucket",List.of("checkSum","mime",key),List.of(MetaDataConstants.LMDB_METADATA_KEY),metaDataList);
        Map<String, MetaData> stringMetaDataMap = testBean.rangeSelect("metadata-test-bucket", List.of(), List.of(), null, "test-medata-range-extra0090","test-medata-range-extra");
        System.out.println(stringMetaDataMap.size());
        System.out.println(stringMetaDataMap.keySet().stream().sorted().toList());
        assert stringMetaDataMap.size() == 91;

        log.info("Metadata-Service-Unit-Test {}/{} , {} Test Success",current,all,"Patch Insert-SelectByRange-GreaterThan");
    }


    @Test
    public void selectMdByMdRange3(){
        current++;

        log.info("Metadata-Service-Unit-Test {}/{} , {} Test Starts",current,all,"Patch Insert-SelectByRange-LessThan");
        String key = "test-metadata";

        List<MetaData> metaDataList = new ArrayList<>();
        List<String> keys = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            MetaData metaData = new MetaData();
            metaData.key = "test-medata-range-detail"+ StringFormatUtil.fillInteger(i,4);
            metaData.setCreateUTC("13123123");
            metaData.setCreateUTC("utc"+StringFormatUtil.fillInteger(i,5));
            metaData.setMIME(UUID.randomUUID().toString());
            metaData.metaDataMap.put(key,UUID.randomUUID().toString());
            metaDataList.add(metaData);
        }

        testBean.patchInsert("metadata-test-bucket",List.of("checkSum","mime",key,"createUTC"),List.of(MetaDataConstants.LMDB_METADATA_KEY),metaDataList);
        Map<String, MetaData> stringMetaDataMap = testBean.detailRangeSelect("metadata-test-bucket", List.of(), List.of(), "createUTC","utc00010", "utc00080","utc");
        System.out.println(stringMetaDataMap.size());
        System.out.println(stringMetaDataMap.keySet().stream().sorted().toList());
        assert stringMetaDataMap.size() == 71;

        log.info("Metadata-Service-Unit-Test {}/{} , {} Test Success",current,all,"Patch Insert-SelectByRange-GreaterThan");
    }

    @Test
    public void deleteSingleRollback() throws ExecutionException, InterruptedException {
        current++;
        log.info("Metadata-Service-Unit-Test {}/{} , {} Test Starts",current,all,"Single Delete-Select");

        for (int i = 0; i < 10; i++) {
            Future<?> submit = executor.submit(() -> {
                MetaData metaData = new MetaData();
                metaData.key = "test-medata-key-rollback";
                metaData.setCheckSum(UUID.randomUUID().toString());
                metaData.setMIME(UUID.randomUUID().toString());
                String key = UUID.randomUUID().toString();
                metaData.metaDataMap.put(key, UUID.randomUUID().toString());

                testBean.singleInsert("metadata-test-bucket",List.of("checkSum","mime",key,"createUTC"),List.of(MetaDataConstants.LMDB_METADATA_KEY),metaData);

                try {
                    testBean.deleteNewDataMeRollback("metadata-test-bucket", List.of("checkSum", "mime", key), List.of(), metaData);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }

                Map<String, MetaData> stringMetaDataMap = testBean.patchSelect("metadata-test-bucket", List.of(), List.of(), List.of("test-medata-key-rollback"));
                assert stringMetaDataMap.get("test-medata-key-rollback") != null;
            });
            submit.get();
        }

        log.info("Metadata-Service-Unit-Test {}/{} , {} Test Success",current,all,"Single Delete-Select");
    }

    @Test
    public void insertPatchRollback() throws InterruptedException, ExecutionException {
        current++;
        log.info("Metadata-Service-Unit-Test {}/{} , {} Test Starts",current,all,"Patch Insert-Select-1");

        for (int q = 0; q < 10; q++) {
            Future<?> submit = executor.submit(
                    () -> {
                        List<String> keys = new ArrayList<>();
                        for (int i = 0; i < 100; i++) {
                            MetaData metaData = new MetaData();
                            metaData.key = "test-medata-key-rollback-patch1" + i;
                            metaData.setCheckSum("@3");
                            metaData.setMIME("12312");
                            String key = "test-metadata";
                            metaData.metaDataMap.put(key, "13123");
                            try {
                                testBean.singleInsertRollback("metadata-test-bucket", List.of("checkSum", "mime", key), List.of(), metaData);
                            } catch (Exception e) {
                                throw new RuntimeException(e);
                            }
                        }

                        Map<String, MetaData> stringMetaDataMap = testBean.patchSelect("metadata-test-bucket", List.of(), List.of(), keys);
                        assert stringMetaDataMap.isEmpty();
                    }
            );
            submit.get();
        }


        log.info("Metadata-Service-Unit-Test {}/{} , {} Test Success",current,all,"Patch Insert-Select-1");

    }

    @Test
    public void insertPatch2Rollback(){
        current++;

        log.info("Metadata-Service-Unit-Test {}/{} , {} Test Starts",current,all,"Patch Insert-Select-2");
        String key = "test-metadata";

        List<MetaData> metaDataList = new ArrayList<>();
        List<String> keys = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            MetaData metaData = new MetaData();
            metaData.key = "test-medata-key-rollback-patch2-"+i;
            metaData.setCheckSum("@3");
            metaData.setMIME("12312");
            metaData.metaDataMap.put(key,"13123");
            metaDataList.add(metaData);
        }

        try {
            testBean.patchInsertRollback("metadata-test-bucket",List.of("checkSum","mime",key),List.of(),metaDataList);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        Map<String, MetaData> stringMetaDataMap = testBean.patchSelect("metadata-test-bucket", List.of(), List.of(), keys);
        assert stringMetaDataMap.isEmpty();

        log.info("Metadata-Service-Unit-Test {}/{} , {} Test Success",current,all,"Patch Insert-Select-2");
    }

    @Test
    public void addNewMetadataRollback() throws ExecutionException, InterruptedException {
        current++;
        log.info("Metadata-Service-Unit-Test {}/{} , {} Test Starts",current,all,"Patch Insert-PrefixSelect");

        for (int q = 0; q < 10; q++) {
            Future<?> submit = executor.submit(() -> {
                String key = "test-metadata";

                List<MetaData> metaDataList = new ArrayList<>();
                List<String> keys = new ArrayList<>();
                for (int i = 0; i < 10; i++) {
                    MetaData metaData = new MetaData();
                    metaData.key = "test-newData-add-rollback" + i;
                    keys.add(metaData.key);
                    metaData.setCheckSum("@3");
                    metaData.setMIME("12312");
                    metaData.metaDataMap.put(key, "13123");
                    metaDataList.add(metaData);
                }

                MetaData metaData = new MetaData();
                keys.add(metaData.key);
                metaData.setCheckSum("@3");
                metaData.setMIME("12312");
                metaData.metaDataMap.put(key, "13123");

                try {
                    testBean.addNewDetailRollback("metadata-test-bucket", List.of("checkSum", "mime", key,"123"), List.of(),metaData);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
                Map<String, MetaData> stringMetaDataMap = testBean.patchSelect("metadata-test-bucket", List.of(), List.of(), keys);

                for (MetaData value : stringMetaDataMap.values()) {
                    assert !value.metaDataMap.get("123").equals("123");
                }
            });
            submit.get();
        }

        log.info("Metadata-Service-Unit-Test {}/{} , {} Test Success",current,all,"Patch Insert-PrefixSelect");

    }
}