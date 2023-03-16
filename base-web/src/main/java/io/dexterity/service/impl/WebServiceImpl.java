package io.dexterity.service.impl;

import cn.hutool.core.convert.Convert;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import io.dexterity.MetaDataApi;
import io.dexterity.StorageApi;
import io.dexterity.annotation.RocksDBTransactional;
import io.dexterity.dao.WebDao;
import io.dexterity.po.vo.ChunkVO;
import io.dexterity.po.vo.RocksDBVo;
import io.dexterity.service.WebService;
import lombok.extern.slf4j.Slf4j;
import org.rocksdb.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

@Service
@Slf4j
public class WebServiceImpl extends ServiceImpl<WebDao, ChunkVO> implements WebService {
    @Autowired
    private StorageApi storageApi;
    @Autowired
    private MetaDataApi metaDataApi;
    @Autowired
    private WebDao webDao;
    @Override
    public Integer saveChunk(MultipartFile chunk, Integer index,
                             Integer chunkTotal, Long chunkSize,
                             String crypto, String bucketName,
                             String fileName,Long fileSize,String chunkCrypto) throws RocksDBException, IOException {
        // 分块信息先临时存在derby中，CHUNK_INFO表 TODO 后面要存到lmdb里面(key-crypto,value-Map)
        webDao.insert(new ChunkVO(index,chunkTotal,chunkSize,bucketName));
        // 分块数据以（crypto-chunk）存RocksDB中
        storageApi.put(new RocksDBVo(bucketName, Convert.toPrimitiveByteArray(crypto),chunk.getBytes()));
        return 1;
    }

    public int checkChunkAll() throws RocksDBException {
        return storageApi.getAllKey("chunkTmp").size();
    }

    @Override
    public byte[] mergeChunk() throws RocksDBException, IOException {
        RocksIterator iterator = storageApi.getIterator("chunkTmp");
        iterator.seekToFirst();
        String tempDir = System.getenv("TEMP"); // 获取临时目录的路径
        Path tempFile = Files.createTempFile(Paths.get(tempDir), "merged", ".dat"); // 创建临时文件
        try (FileChannel channel = FileChannel.open(tempFile, StandardOpenOption.READ, StandardOpenOption.WRITE)) {
            while (iterator.isValid()) {
                byte[] value = storageApi.get("chunkTmp", iterator.key()).getValue();
                ByteBuffer buffer = ByteBuffer.wrap(value);
                channel.write(buffer); // 将块写入临时文件中
                iterator.next();
            }
        }
        byte[] mergedData = null;
        int segmentSize = 4096; // 段的大小为4KB
        try (FileChannel channel = FileChannel.open(tempFile, StandardOpenOption.READ)) {
            int fileSize = (int) channel.size();
            int segmentCount = (fileSize + segmentSize - 1) / segmentSize; // 计算需要分成多少段
            mergedData = new byte[fileSize];
            ByteBuffer buffer = ByteBuffer.allocate(segmentSize); // 创建一个大小为4KB的ByteBuffer
            for (int i = 0; i < segmentCount; i++) {
                int position = i * segmentSize;
                int remaining = fileSize - position;
                int size = Math.min(segmentSize, remaining); // 计算当前段的大小
                buffer.clear();
                channel.read(buffer, position); // 从文件中读取当前段的数据
                buffer.flip();
                buffer.get(mergedData, position, size); // 将当前段的数据追加到mergedData中
            }
        }
        log.info("Success Merge!");
        Files.delete(tempFile); // 删除临时文件
        return mergedData;
    }


    @Override
    @RocksDBTransactional
    public int saveObject(byte[] object,String bucketName,String fileName,String md5,Long fileSize) throws RocksDBException {
        TransactionDB txnDB = storageApi.getTransaction();
        Transaction txn1 = txnDB.beginTransaction(new WriteOptions());

        //删除rocksdb中的临时列族,chunkTmp
        storageApi.cfDelete("chunkTmp");
        //删除derby中 的临时信息,CHUNK_INFO
        webDao.deleteChunkTemp();
        //保存对象的数据信息到rocksdb
        storageApi.put(new RocksDBVo(bucketName,fileName.getBytes(),object));
        HashMap<String,String> maps = new HashMap<>();
        maps.put("fileSize",fileSize.toString());
        maps.put("md5",md5);
        int a = 1/0;
        //保存对象的元数据信息到lmdb
//        metaDataApi.insertNewMetadata(new MetaData(fileName,maps),bucketName);
        return 1;
    }

    @Override
    public List<String> getAllObj(String bucketName) throws RocksDBException {
        //从rocksdb中查询
        List<byte[]> keys=storageApi.getAllKey(bucketName);
        //转换byte数组为String字符串
        return keys.stream()
                .map(bytes -> new String(bytes, StandardCharsets.UTF_8))
                .toList();
    }

    @Override
    public int deleteObj(String bucketName, String fileName) throws RocksDBException {
        //从rocksdb中删除该对象
        storageApi.delete(bucketName,fileName.getBytes());
        return 1;
    }

    @Override
    public Boolean findObjByMD5(String md5) {
        return false;
    }

    @Override
    public List<Integer> findChunkListByMD5(String md5) {
        return new ArrayList<>();
    }
}
