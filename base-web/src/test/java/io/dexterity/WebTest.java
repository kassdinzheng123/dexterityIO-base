package io.dexterity;

import io.dexterity.dao.WebDao;
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
    private WebDao webDao;
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

    @Test
    public void test3() throws RocksDBException {
        //删除rocksdb中的临时列族,chunkTmp
        storageApi.cfDelete("chunkTmp");
        //删除derby中的临时信息,CHUNK_INFO
        webDao.deleteChunkTemp();
    }
}
