package io.dexterity.service.impl;

import cn.hutool.crypto.digest.DigestUtil;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import io.dexterity.annotation.*;
import io.dexterity.aspect.LmdbTxn;
import io.dexterity.client.MultipleEnv;
import io.dexterity.dao.WebDao;
import io.dexterity.exception.MyException;
import io.dexterity.po.vo.ChunkVO;
import io.dexterity.service.WebService;
import lombok.extern.slf4j.Slf4j;
import org.lmdbjava.Txn;
import org.rocksdb.RocksDBException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

@Service
@Slf4j
public class WebServiceImpl extends ServiceImpl<WebDao, ChunkVO> implements WebService {
    @Autowired
    private WebDao webDao;

    @LmdbWrite
    @RocksDBTransactional
    @Override
    public Integer saveChunk(MultipartFile chunk, Integer index,
                             Integer chunkTotal, Long chunkSize,
                             String crypto, @BucketName String bucketName,
                             String fileName, Long fileSize, String chunkCrypto,
                             @DupNames List<String> dup, @UnDupNames List<String> unDup) throws RocksDBException, IOException {
        //TODO 远程调用
//        // 分块信息存入LMDB
//        MultipleEnv multipleEnv = LmdbTxn.getEnv(bucketName);
//        Txn<ByteBuffer> txn = LmdbTxn.getWriteTxn(bucketName);
//
//        MetaData metaData = new MetaData();
//        metaData.key=crypto;
//        metaData.metaDataMap.put("fileName",fileName);
//        metaData.metaDataMap.put("fileSize", fileSize.toString());
//        metaData.metaDataMap.put("chunk"+index,chunkCrypto);
//        metaData.metaDataMap.put("chunkTotal", chunkTotal.toString());
//        metaData.metaDataMap.put("bucketName",bucketName);
//        metaDataApi.insertNewMetadata(metaData,multipleEnv,txn);
//
//        // 分块数据存入RocksDB
//        storageApi.put(new RocksDBVo(bucketName, Convert.toPrimitiveByteArray(crypto),chunk.getBytes()));

        // 校验分片的sha256
        if(checkChunk(chunkCrypto,chunk)==1){
            log.info("分片"+index+"校验成功，sha256:"+chunkCrypto);
            return 1;
        }else{
            log.info("分片"+index+"校验失败");
            throw new MyException(500,"分片校验失败");
        }
    }

    public int checkChunkAll() throws RocksDBException {
        // TODO 远程调用
//        storageApi.getAllKey("chunkTmp").size();
        return 1;
    }

    @Override
    public byte[] mergeChunk() throws RocksDBException, IOException {
        // TODO 远程调用
//        RocksIterator iterator = storageApi.getIterator("chunkTmp");
//        iterator.seekToFirst();
//        String tempDir = System.getenv("TEMP"); // 获取临时目录的路径
//        Path tempFile = Files.createTempFile(Paths.get(tempDir), "merged", ".dat"); // 创建临时文件
//        try (FileChannel channel = FileChannel.open(tempFile, StandardOpenOption.READ, StandardOpenOption.WRITE)) {
//            while (iterator.isValid()) {
//                byte[] value = storageApi.get("chunkTmp", iterator.key()).getValue();
//                ByteBuffer buffer = ByteBuffer.wrap(value);
//                channel.write(buffer); // 将块写入临时文件中
//                iterator.next();
//            }
//        }
//        byte[] mergedData = null;
//        int segmentSize = 4096; // 段的大小为4KB
//        try (FileChannel channel = FileChannel.open(tempFile, StandardOpenOption.READ)) {
//            int fileSize = (int) channel.size();
//            int segmentCount = (fileSize + segmentSize - 1) / segmentSize; // 计算需要分成多少段
//            mergedData = new byte[fileSize];
//            ByteBuffer buffer = ByteBuffer.allocate(segmentSize); // 创建一个大小为4KB的ByteBuffer
//            for (int i = 0; i < segmentCount; i++) {
//                int position = i * segmentSize;
//                int remaining = fileSize - position;
//                int size = Math.min(segmentSize, remaining); // 计算当前段的大小
//                buffer.clear();
//                channel.read(buffer, position); // 从文件中读取当前段的数据
//                buffer.flip();
//                buffer.get(mergedData, position, size); // 将当前段的数据追加到mergedData中
//            }
//        }
//        log.info("Success Merge!");
//        Files.delete(tempFile); // 删除临时文件
//        return mergedData;
        return null;
    }


    @Override
    @RocksDBTransactional
    public int saveObject(byte[] object,String bucketName,String fileName,String md5,Long fileSize) throws RocksDBException {
        //TODO 远程调用
//        TransactionDB txnDB = storageApi.getTransaction();
//        Transaction txn1 = txnDB.beginTransaction(new WriteOptions());
//
//        //删除rocksdb中的临时列族,chunkTmp
//        storageApi.cfDelete("chunkTmp");
//        //删除derby中 的临时信息,CHUNK_INFO
//        webDao.deleteChunkTemp();
//        //保存对象的数据信息到rocksdb
//        storageApi.put(new RocksDBVo(bucketName,fileName.getBytes(),object));
//        HashMap<String,String> maps = new HashMap<>();
//        maps.put("fileSize",fileSize.toString());
//        maps.put("md5",md5);
//        //保存对象的元数据信息到lmdb
////        metaDataApi.insertNewMetadata(new MetaData(fileName,maps),bucketName);
        return 1;
    }

    @Override
    public List<String> getAllObj(String bucketName) throws RocksDBException {
        // TODO 远程调用
//        //从rocksdb中查询
//        List<byte[]> keys=storageApi.getAllKey(bucketName);
//        //转换byte数组为String字符串
//        return keys.stream()
//                .map(bytes -> new String(bytes, StandardCharsets.UTF_8))
//                .toList();
        return null;
    }

    @Override
    public int deleteObj(String bucketName, String fileName) throws RocksDBException {
        // TODO 远程调用
//        //从rocksdb中删除该对象
//        storageApi.delete(bucketName,fileName.getBytes());
        return 1;
    }

    @Override
    public Boolean findObjByCrypto(String crypto,String bucketName) {
        MultipleEnv multipleEnv = LmdbTxn.getEnv(bucketName);
        Txn<ByteBuffer> txnRead = LmdbTxn.getReadTxn(bucketName);
        return false;
    }

    @Override
    public List<Integer> findChunkListByMD5(String md5) {
        return new ArrayList<>();
    }

    @Override
    public int checkChunk(String chunkCrypto, MultipartFile chunk) throws IOException {
        if (Objects.equals(chunkCrypto,DigestUtil.sha256Hex(chunk.getBytes()))){
            return 1;
        }
        return 0;
    }
}
