//package io.dexterity.service.impl;
//
//import io.dexterity.annotation.*;
//import io.dexterity.aspect.LmdbTxn;
//import io.dexterity.client.MultipleEnv;
//import lombok.extern.slf4j.Slf4j;
//import org.lmdbjava.Txn;
//import org.springframework.beans.factory.annotation.Autowired;
//import org.springframework.stereotype.Service;
//
//import java.nio.ByteBuffer;
//import java.util.List;
//import java.util.Map;
//import java.util.Set;
//
//@Service
//@Slf4j
//public class TestBean {
//
//    @Autowired
//    LmdbMetaDataService lmdbMetaDataService;
//
//
//
//    @LmdbWrite
//    public void emptyDelete(@BucketName String bucket,
//                            @DupNames List<String> dup,
//                            @UnDupNames List<String> unDup,
//                            MetaData metaData){
//        MultipleEnv multipleEnv = LmdbTxn.getEnv(bucket);
//        Txn<ByteBuffer> txn = LmdbTxn.getWriteTxn(bucket);
//        Txn<ByteBuffer> txnRead = LmdbTxn.getReadTxn(bucket);
//        var strings = lmdbMetaDataService.selectMdByKeys(List.of(metaData.key), multipleEnv, txnRead);
//        assert strings.isEmpty();
//        lmdbMetaDataService.deleteMetadata(List.of(metaData.key), multipleEnv, txn);
//        strings = lmdbMetaDataService.selectMdByKeys(List.of(metaData.key), multipleEnv, txnRead);
//        assert strings.isEmpty();
//    }
//
//    @LmdbWrite
//    public void singleInsert(@BucketName String bucket,
//                            @DupNames List<String> dup,
//                            @UnDupNames List<String> unDup,
//                            MetaData metaData){
//        MultipleEnv multipleEnv = LmdbTxn.getEnv(bucket);
//        Txn<ByteBuffer> txn = LmdbTxn.getWriteTxn(bucket);
//        Txn<ByteBuffer> txnRead = LmdbTxn.getReadTxn(bucket);
//
//        lmdbMetaDataService.insertNewMetadata(metaData,multipleEnv,txn);
//    }
//
//    @LmdbWrite
//    public void singleInsertRollback(@BucketName String bucket,
//                             @DupNames List<String> dup,
//                             @UnDupNames List<String> unDup,
//                             MetaData metaData) throws Exception {
//        MultipleEnv multipleEnv = LmdbTxn.getEnv(bucket);
//        Txn<ByteBuffer> txn = LmdbTxn.getWriteTxn(bucket);
//        Txn<ByteBuffer> txnRead = LmdbTxn.getReadTxn(bucket);
//
//        lmdbMetaDataService.insertNewMetadata(metaData,multipleEnv,txn);
//        throw new Exception();
//    }
//
//    @LmdbWrite
//    public void patchInsert(@BucketName String bucket,
//                             @DupNames List<String> dup,
//                             @UnDupNames List<String> unDup,
//                             List<MetaData> metaData){
//        MultipleEnv multipleEnv = LmdbTxn.getEnv(bucket);
//        Txn<ByteBuffer> txn = LmdbTxn.getWriteTxn(bucket);
//        Txn<ByteBuffer> txnRead = LmdbTxn.getReadTxn(bucket);
//
//        lmdbMetaDataService.insertPatch(metaData,multipleEnv,txn);
//    }
//
//    @LmdbWrite
//    public void patchInsertRollback(@BucketName String bucket,
//                            @DupNames List<String> dup,
//                            @UnDupNames List<String> unDup,
//                            List<MetaData> metaData) throws Exception {
//        MultipleEnv multipleEnv = LmdbTxn.getEnv(bucket);
//        Txn<ByteBuffer> txn = LmdbTxn.getWriteTxn(bucket);
//        Txn<ByteBuffer> txnRead = LmdbTxn.getReadTxn(bucket);
//
//        lmdbMetaDataService.insertPatch(metaData,multipleEnv,txn);
//        throw new Exception();
//    }
//
//    @LmdbWrite
//    public void addNewDetailRollback(@BucketName String bucket,
//                            @DupNames List<String> dup,
//                            @UnDupNames List<String> unDup,
//                            MetaData metaData) throws Exception {
//        MultipleEnv multipleEnv = LmdbTxn.getEnv(bucket);
//        Txn<ByteBuffer> txn = LmdbTxn.getWriteTxn(bucket);
//        Txn<ByteBuffer> txnRead = LmdbTxn.getReadTxn(bucket);
//
//        lmdbMetaDataService.addNewMetadata(metaData,multipleEnv,txn,"123","123");
//        throw new Exception();
//    }
//
//    @LmdbWrite
//    public void addNewDetail(@BucketName String bucket,
//                                     @DupNames List<String> dup,
//                                     @UnDupNames List<String> unDup,
//                                     MetaData metaData)  {
//        MultipleEnv multipleEnv = LmdbTxn.getEnv(bucket);
//        Txn<ByteBuffer> txn = LmdbTxn.getWriteTxn(bucket);
//        Txn<ByteBuffer> txnRead = LmdbTxn.getReadTxn(bucket);
//
//        lmdbMetaDataService.addNewMetadata(metaData,multipleEnv,txn,"123","123");
//    }
//
//    @LmdbRead
//    public Map<String, MetaData> singleSelect(@BucketName String bucket,
//                             @DupNames List<String> dup,
//                             @UnDupNames List<String> unDup,
//                             MetaData metaData){
//        MultipleEnv multipleEnv = LmdbTxn.getEnv(bucket);
//        Txn<ByteBuffer> txnRead = LmdbTxn.getReadTxn(bucket);
//
//        return lmdbMetaDataService.selectMdByKeys(List.of(metaData.key), multipleEnv, txnRead);
//    }
//
//    @LmdbRead
//    public Map<String, MetaData> patchSelect(@BucketName String bucket,
//                                              @DupNames List<String> dup,
//                                              @UnDupNames List<String> unDup,
//                                              List<String> keys){
//        MultipleEnv multipleEnv = LmdbTxn.getEnv(bucket);
//        Txn<ByteBuffer> txnRead = LmdbTxn.getReadTxn(bucket);
//
//        return lmdbMetaDataService.selectMdByKeys(keys, multipleEnv, txnRead);
//    }
//
//    @LmdbRead
//    public Set<String> patchSelectByMd(@BucketName String bucket,
//                                       @DupNames List<String> dup,
//                                       @UnDupNames List<String> unDup,
//                                       MetaData metaData){
//        MultipleEnv multipleEnv = LmdbTxn.getEnv(bucket);
//        Txn<ByteBuffer> txnRead = LmdbTxn.getReadTxn(bucket);
//
//        return lmdbMetaDataService.selectByMetaData(metaData, multipleEnv, txnRead);
//    }
//
//    @LmdbRead
//    public Map<String, MetaData> rangeSelect(@BucketName String bucket,
//                                             @DupNames List<String> dup,
//                                             @UnDupNames List<String> unDup,
//                                             String lb, String ub,String prefix){
//        MultipleEnv multipleEnv = LmdbTxn.getEnv(bucket);
//        Txn<ByteBuffer> txnRead = LmdbTxn.getReadTxn(bucket);
//
//        return lmdbMetaDataService.selectMdByKeyRange(lb,ub, prefix,multipleEnv, txnRead);
//    }
//
//    @LmdbRead
//    public Map<String, MetaData> detailRangeSelect(@BucketName String bucket,
//                                             @DupNames List<String> dup,
//                                             @UnDupNames List<String> unDup,String detail,
//                                             String lb, String ub,String prefix){
//        MultipleEnv multipleEnv = LmdbTxn.getEnv(bucket);
//        Txn<ByteBuffer> txnRead = LmdbTxn.getReadTxn(bucket);
//
//        return lmdbMetaDataService.selectMdByMdRange(detail,lb,ub, prefix,multipleEnv, txnRead);
//    }
//
//    @LmdbRead
//    public Map<String, MetaData> prefixSelect(@BucketName String bucket,
//                                             @DupNames List<String> dup,
//                                             @UnDupNames List<String> unDup,
//                                             String prefix){
//        MultipleEnv multipleEnv = LmdbTxn.getEnv(bucket);
//        Txn<ByteBuffer> txnRead = LmdbTxn.getReadTxn(bucket);
//
//        return lmdbMetaDataService.selectMdByKeyPrefix(prefix, multipleEnv, txnRead);
//    }
//
//
//
//    @LmdbWrite
//    public void deleteNewDataMe(@BucketName String bucket,
//                                    @DupNames List<String> dup,
//                                    @UnDupNames List<String> unDup,
//                                    MetaData metaData){
//        MultipleEnv multipleEnv = LmdbTxn.getEnv(bucket);
//        Txn<ByteBuffer> txn = LmdbTxn.getWriteTxn(bucket);
//        Txn<ByteBuffer> txnRead = LmdbTxn.getReadTxn(bucket);
//        lmdbMetaDataService.deleteMetadata(List.of(metaData.key), multipleEnv, txn);
//    }
//
//    @LmdbWrite
//    public void deleteNewDataMeRollback(@BucketName String bucket,
//                                @DupNames List<String> dup,
//                                @UnDupNames List<String> unDup,
//                                MetaData metaData) throws Exception {
//        MultipleEnv multipleEnv = LmdbTxn.getEnv(bucket);
//        Txn<ByteBuffer> txn = LmdbTxn.getWriteTxn(bucket);
//        Txn<ByteBuffer> txnRead = LmdbTxn.getReadTxn(bucket);
//        lmdbMetaDataService.deleteMetadata(List.of(metaData.key), multipleEnv, txn);
//        throw new Exception("");
//    }
//
//}
