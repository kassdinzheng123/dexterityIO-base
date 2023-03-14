package io.dexterity.client;

import cn.hutool.core.map.MapUtil;
import io.dexterity.common.client.MultipleDBi;
import io.dexterity.common.client.MultipleEnv;
import io.dexterity.common.client.MultipleLmdb;
import io.dexterity.common.client.entity.LMDBEnvSettings;
import io.dexterity.common.client.entity.LMDBEnvSettingsBuilder;
import io.dexterity.common.entity.MetaData;
import io.dexterity.common.entity.constants.MetaDataConstants;
import org.junit.jupiter.api.Test;
import org.lmdbjava.Cursor;
import org.lmdbjava.Env;
import org.lmdbjava.Txn;

import java.lang.reflect.Parameter;
import java.nio.ByteBuffer;
import java.util.*;

public class LMDBTest {

    @Test
    public void expandTest() throws MultipleEnv.LMDBCreateFailedException {
        MultipleLmdb.initMainEnv();
        LMDBEnvSettings test = LMDBEnvSettingsBuilder.startBuild()
                .envName("test")
                .maxDBInstance(1)
                .maxReaders(100)
                .maxSize(1024L * 1024L)
                .filePosition("D:\\Resource\\lmdb-test").build();

        MultipleEnv multipleEnv1 = MultipleLmdb.buildNewEnv(test);
        Env<ByteBuffer> env = multipleEnv1.getEnv();
        MultipleEnv multipleEnv = new MultipleEnv("test", env);
        MultipleDBi multipleDBi = multipleEnv.
                buildDBInstance("db-test", false,
                        false);

        for (int j = 0; j < 100; j++) {
            Map<String, List<String>> testList = new HashMap<>();
            for (int i = 0; i < 10000; i++) {
                testList.put(String.valueOf(UUID.randomUUID()),
                        Collections.singletonList("dasdweweqeqweqewq21321321"));
            }
            long startTime = System.currentTimeMillis();
            multipleDBi.putAll(testList);
            long endTime = System.currentTimeMillis();
            long elapsedTime = endTime - startTime;
            System.out.println("Elapsed time in milliseconds: " + elapsedTime);
        }
    }

    @Test
    public void testForDupData() throws MultipleEnv.LMDBCreateFailedException {
        long initStart = System.currentTimeMillis();
        MultipleLmdb.initMainEnv();
        long initEndTime = System.currentTimeMillis();
        long initTime = initEndTime - initStart;
        System.out.println("init use times: " + initTime);
//        LMDBEnvSettings test = LMDBEnvSettingsBuilder.startBuild()
//                .envName("test")
//                .maxDBInstance(1)
//                .maxReaders(100)
//                .maxSize(1024 * 1024 * 1000)
//                .filePosition("D:\\Resource\\lmdb-test").build();

        MultipleEnv multipleEnv = MultipleLmdb.envs.get("test");

//        LMDBClientCollection lmdbClientCollection = LMDBEnvCollection.buildNewEnv(test);
//        Env<ByteBuffer> env = lmdbClientCollection.getEnv();
//        LMDBClient lmdbClient = lmdbClientCollection.
//                buildDBInstance("db-test", false,
//                        true);
        MultipleDBi multipleDBi = multipleEnv.getLmdbMaps().get("db-test");
        Map<String, List<String>> testList = new HashMap<>();
        String key = UUID.randomUUID().toString();
        for (int i = 0; i < 100; i++) {
            testList.put(key, Collections.singletonList(UUID.randomUUID().toString()));
        }

        for (int i = 0; i < 1000000; i++) {
            testList.put(UUID.randomUUID().toString(), Collections.singletonList(UUID.randomUUID().toString()));
        }

        multipleDBi.putAll(testList);

        long startTime = System.currentTimeMillis();

        List<String> duplicatedData = multipleDBi.getDuplicatedData(key);

        long endTime = System.currentTimeMillis();
        long elapsedTime = endTime - startTime;
        System.out.println("Duplicated Data Search time in milliseconds: " + elapsedTime);
    }

    @Test
    public void preBuildDB() throws MultipleEnv.LMDBCreateFailedException {
        MultipleEnv multipleEnv = MultipleLmdb.envs.get("metadata-test-bucket");
        MultipleDBi metaDBi = multipleEnv.buildDBInstance(String.valueOf(21912), false, true);

    }

    @Test
    public void testForSubTxn() throws MultipleEnv.LMDBCreateFailedException {


        MultipleEnv multipleEnv = MultipleLmdb.envs.get("metadata-test-bucket");

        multipleEnv.buildDBInstance(String.valueOf(101912), false, true);
        multipleEnv.buildDBInstance(String.valueOf(2 * 101912), false, true);


        MultipleLmdb.checkAndExpand("metadata-test-bucket", "1231231221123");

        MultipleDBi multipleDBi = multipleEnv.buildDBInstance(MetaDataConstants.LMDB_METADATA_KEY, false, false);

        Env<ByteBuffer> env = multipleEnv.getEnv();
        try (Txn<ByteBuffer> txn = env.txnWrite()) {

            multipleDBi.putJsonObject("12312312211123", "12312131221123", txn);


            for (int q = 1; q < 3; q++) {
                MultipleDBi metaDBi = multipleEnv.buildDBInstance(String.valueOf(q * 101912), false, true);
                try (Txn<ByteBuffer> ct = env.txn(txn)) {
                    Cursor<ByteBuffer> c = metaDBi.db.openCursor(ct);
                    for (int i = 0; i < 100; i++) {
                        ByteBuffer keyBuffer = ByteBuffer.allocateDirect(12);
                        ByteBuffer valueBuffer = ByteBuffer.allocateDirect(12);
                        c.put(keyBuffer, valueBuffer);
                        c.next();
                    }
                    c.close();
                    ct.commit();
                } catch (Exception e) {
                    System.out.println("error");
                }
            }

            txn.commit();
        } catch (MultipleEnv.LMDBCreateFailedException e) {
            throw new RuntimeException(e);
        }

    }

    @Test
    public void testForEncryption(){
        MultipleEnv multipleEnv = MultipleLmdb.getEnvs().get("mainEnv");
        try {
            MultipleDBi multipleDBi = multipleEnv.buildDBInstance("main", false, false);
            System.out.println(multipleDBi.stringKey(multipleDBi.byteKey("kassdin")));
        } catch (MultipleEnv.LMDBCreateFailedException e) {
            throw new RuntimeException(e);
        }

    }

    @Test
    public void testForKeyRange() throws MultipleEnv.LMDBCreateFailedException {



        MultipleEnv multipleEnv = MultipleLmdb.envs.get("metadata-test-bucket");

        multipleEnv.buildDBInstance("test",false,true);
        try{
             MultipleDBi multipleDBi = multipleEnv.buildDBInstance("test", false, false);

             Map<String,List<String>> data = new HashMap<>();
             data.put("prefix-test1",List.of("123123"));
             data.put("prefix-test2",List.of("123123"));
             data.put("prefix-test3",List.of("123123"));
             data.put("prefix-test4",List.of("12312"));
             data.put("12312312123",List.of("12312312"));
             multipleDBi.putAll(data);
            List<Map.Entry<String, String>> prefix = multipleDBi.prefixSearch("prefix");
            List<Map.Entry<String, String>> all = multipleDBi.getAll();
            System.out.println(all);
            System.out.println(prefix);
        } catch (MultipleEnv.LMDBCreateFailedException e) {
            throw new RuntimeException(e);
        }

    }

}


