package io.dexterity;

import io.dexterity.client.DerbyClient;
import io.dexterity.dao.WebDao;
import io.dexterity.po.vo.RocksDBVo;
import io.dexterity.service.WebService;
import org.junit.jupiter.api.Test;
import org.rocksdb.RocksDBException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.sql.SQLException;

@SpringBootTest
public class WebTest {
    @Autowired
    private StorageApi storageApi;
    @Autowired
    private WebDao webDao;
    @Autowired
    private WebService webService;



    @Test
    public void query() throws RocksDBException, SQLException {
        //RocksDB数据库查询
        System.out.println(storageApi.cfAll());
        System.out.println("default列族的总条数："+storageApi.getCount("default"));
        System.out.println("default列族所有的Key："+storageApi.getAllKey("default"));

        System.out.println("test列族的总条数："+storageApi.getCount("test"));
        System.out.println("test列族所有的Key："+storageApi.getAllKey("test"));

        //Derby数据库查询
        DerbyClient derbyClient = new DerbyClient();
        System.out.println("derby数据库中的所有表：");
        derbyClient.listTable();
        System.out.println("BUCKET表的总条数："+derbyClient.selectCount("BUCKET"));
        System.out.println("CHUNK_INFO表的总条数："+derbyClient.selectCount("CHUNK_INFO"));
        derbyClient.close();
    }

    @Test
    public void clear() throws RocksDBException {
        //删除rocksdb中的临时列族,chunkTmp
        storageApi.cfDelete("chunkTmp");
        //删除derby中的临时信息,CHUNK_INFO
        webDao.deleteChunkTemp();
    }

    @Test
    public void cf() throws RocksDBException {
        //列族查询
        storageApi.cfAll();
        //列族添加
        storageApi.cfAdd("bucket1");
        //列族删除
        storageApi.cfDelete("bucket1");
    }


    @Test
    void objCRUD() throws RocksDBException {
        String key = "1234";
        String value = "12345";
        //增
        RocksDBVo rocksDBVo = new RocksDBVo("default",key.getBytes(),value.getBytes());
        storageApi.put(rocksDBVo);
        //查
        RocksDBVo rocksDBVo1 = storageApi.get("default",key.getBytes());
        System.out.println(rocksDBVo1);
        //删
        RocksDBVo rocksDBVo2 = storageApi.delete("bucket",key.getBytes());
        System.out.println(rocksDBVo2);
    }
}
