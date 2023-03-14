package io.dexterity;

import io.dexterity.service.WebService;
import org.junit.jupiter.api.Test;
import org.rocksdb.RocksDBException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.mock.web.MockMultipartFile;

import java.io.IOException;

@SpringBootTest
public class WebTest {
    @Autowired
    private StorageApi storageApi;

    @Autowired
    private WebService webService;
    @Test
    public void test(){
        System.out.println(storageApi.cfAll());
    }

    @Test
    public void test2() throws RocksDBException, IOException {
        byte[] content = "test file content".getBytes();
        MockMultipartFile file = new MockMultipartFile("test.txt", "test.txt", "text/plain", content);
        webService.saveChunk(file,1,1,10485760L,"bucket");
    }
}
