package io.dexterity;

import cn.hutool.crypto.digest.DigestUtil;
import io.dexterity.client.DerbyClient;
import io.dexterity.config.MyConfig;
import io.dexterity.dao.WebDao;
import io.dexterity.po.vo.RocksDBVo;
import io.dexterity.service.WebService;
import org.junit.jupiter.api.Test;
import org.rocksdb.RocksDBException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.io.File;
import java.sql.SQLException;

@SpringBootTest
public class WebTest {
    @Autowired
    private StorageApi storageApi;
    @Autowired
    private WebDao webDao;
    @Autowired
    private WebService webService;
    @Autowired
    MyConfig myConfig;
    @Test
    public void init() throws SQLException {
        DerbyClient derbyClient = new DerbyClient();
        storageApi.cfAll();
//        derbyClient.deleteTable("BUCKET");
        derbyClient.createTable(
                "bucket",
                "bucket_id VARCHAR(255) PRIMARY KEY NOT NULL",
                "bucket_name VARCHAR(255) NOT NULL UNIQUE",
                "access_authority VARCHAR(255) NOT NULL",
                "domain_name VARCHAR(255) NOT NULL",
                "region VARCHAR(255) NOT NULL",
                "status INT NOT NULL",
                "create_time VARCHAR(255) NOT NULL",
                "tags VARCHAR(255)");
        derbyClient.listTable();
        derbyClient.close();
    }
    @Test
    public void query() throws RocksDBException, SQLException {
        //RocksDB数据库查询
        System.out.println(storageApi.cfAll());
        System.out.println("default列族的总条数："+storageApi.getCount("default"));
        System.out.println("default列族所有的Key："+storageApi.getAllKey("default"));

        //Derby数据库查询
        DerbyClient derbyClient = new DerbyClient();
        System.out.println("derby数据库中的所有表：");
        derbyClient.listTable();
        System.out.println("BUCKET表的总条数："+derbyClient.selectCount("BUCKET"));
        derbyClient.close();
    }

    @Test
    public void clear() throws RocksDBException {
        //删除rocksdb中的临时列族,chunkTmp
        storageApi.cfDelete("chunkTmp");
        storageApi.cfDelete("bucket1");
        storageApi.cfDelete("bucket2");
        storageApi.cfDelete("test");

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
    @Test
    void getSha256(){
        String sha256 = DigestUtil.sha256Hex(new File("C:\\Users\\warfr\\Downloads\\test.txt"));
        System.out.println(sha256);
    }
}
