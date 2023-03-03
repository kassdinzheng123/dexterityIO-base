package io.dexterity.client;

import io.dexterity.client.entity.LMDBEnvSettings;
import io.dexterity.client.entity.LMDBEnvSettingsBuilder;
import org.junit.jupiter.api.Test;
import org.lmdbjava.Env;

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
                .maxSize(1024 * 1024 * 1)
                .filePosition("D:\\Resource\\lmdb-test").build();

        MultipleEnv multipleEnv1 = MultipleLmdb.buildNewEnv(test);
        Env<ByteBuffer> env = multipleEnv1.getEnv();
        MultipleEnv multipleEnv = new MultipleEnv("test",env);
        MultipleDBi multipleDBi = multipleEnv.
                buildDBInstance("db-test", false,
                        false);

        for (int j = 0; j < 100; j++) {
            List<Map.Entry<String,String>> testList = new ArrayList<>();
            for (int i = 0; i < 10000; i++) {
                testList.add(new AbstractMap.SimpleEntry<>(String.valueOf(UUID.randomUUID()),"dasdweweqeqweqewq21321321"));
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
        System.out.println("init use times: "+initTime);
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
        List<Map.Entry<String,String>> testList = new ArrayList<>();
        String key = UUID.randomUUID().toString();
        for (int i = 0; i < 100; i++) {
            testList.add(new AbstractMap.SimpleEntry<>(key,UUID.randomUUID().toString()));
        }

        for (int i = 0; i < 1000000; i++) {
            testList.add(new AbstractMap.SimpleEntry<>(UUID.randomUUID().toString(),UUID.randomUUID().toString()));
        }

        multipleDBi.putAll(testList);

        long startTime = System.currentTimeMillis();

        List<String> duplicatedData = multipleDBi.getDuplicatedData(key);

        long endTime = System.currentTimeMillis();
        long elapsedTime = endTime - startTime;
        System.out.println("Duplicated Data Search time in milliseconds: " + elapsedTime);

    }
}
