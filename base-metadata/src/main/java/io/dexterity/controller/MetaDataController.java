package io.dexterity.controller;

import io.dexterity.client.MultipleEnv;
import io.dexterity.entity.MetaData;
import io.dexterity.po.R;
import io.dexterity.service.MetaDataService;
import org.lmdbjava.Txn;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.nio.ByteBuffer;
import java.util.List;

@RestController
@RequestMapping("/metadata")
public class MetaDataController {
    @Autowired
    MetaDataService metaDataService;

    public R<?> insertNewMetadata(MetaData metaData, MultipleEnv env, Txn<ByteBuffer> parent){
        return new R<>(200,"请求成功");
    }
    public R<?> deleteMetadata(List<String> metadataKey, MultipleEnv multipleEnv, Txn<ByteBuffer> parent){
        return new R<>(200,"请求成功");
    }
    public R<?> addNewMetadata(MetaData matcher, MultipleEnv multipleEnv, Txn<ByteBuffer> parent, String newMdKey, String newMdValue){
        return new R<>(200,"请求成功");
    }
    public R<?> selectByMetaData(MetaData metaData, MultipleEnv multipleEnv, Txn<ByteBuffer> parent){
        return new R<>(200,"请求成功");
    }
    public R<?> insertPatch(List<MetaData> metaData,  MultipleEnv multipleEnv,Txn<ByteBuffer> parent){
        return new R<>(200,"请求成功");
    }
    public R<?> selectMdByKeys(List<String> key, MultipleEnv multipleEnv, Txn<ByteBuffer> parent){
        return new R<>(200,"请求成功");
    }
    public R<?> selectMdByMdRange(String metadataKey, String lb,String ub, String prefix, MultipleEnv multipleEnv, Txn<ByteBuffer> parent){
        return new R<>(200,"请求成功");
    }
    public R<?> selectMdByKeyRange(String lb,String ub, String prefix, MultipleEnv multipleEnv, Txn<ByteBuffer> parent){
        return new R<>(200,"请求成功");
    }
    public R<?> selectMdByKeyPrefix(String prefix, MultipleEnv multipleEnv,Txn<ByteBuffer> parent){
        return new R<>(200,"请求成功");
    }
}
