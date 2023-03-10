package io.dexterity.controller;

import io.dexterity.BucketApi;
import io.dexterity.StorageApi;
import io.dexterity.po.pojo.R;
import io.dexterity.po.vo.BucketVO;
import io.dexterity.service.WebService;
import io.dexterity.utils.FileUtil;
import lombok.extern.slf4j.Slf4j;
import org.rocksdb.RocksDBException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

@RestController
@RequestMapping("/web")
@Slf4j
public class WebController {
    @Autowired
    private BucketApi bucketApi;
    @Autowired
    private StorageApi storageApi;
    @Autowired
    private WebService webService;

    @GetMapping("/bucket")
    public R<?> listBucket(){
        return new R<>(200,"请求成功",bucketApi.listBucket());
    }

    @PostMapping("/bucket")
    public R<?> createBucket(@RequestBody BucketVO bucket){
        return new R<>(200,"请求成功",bucketApi.createBucket(bucket));
    }

    @DeleteMapping("/bucket")
    public R<?> deleteBucket(@RequestParam("bucketId")Integer bucketId){
        return new R<>(200,"请求成功",bucketApi.deleteBucket(bucketId));
    }

    @PutMapping("/bucket")
    public R<?> updateBucketStatus(@RequestParam("bucketId")Integer bucketId,
                                  @RequestParam("status")Integer status){
        return new R<>(200,"请求成功",bucketApi.updateStatusBucket(bucketId,status));
    }
    @PutMapping("/object")
    public R<?> uploadToBucket(
            @RequestParam("chunk") MultipartFile chunk,//块的数据
            @RequestParam("md5") String md5,//文件的md5值
            @RequestParam("index") Integer index, //块的序号
            @RequestParam("chunkTotal") Integer chunkTotal, //块的总数
            @RequestParam("fileSize") Long fileSize, //文件大小
            @RequestParam("fileName") String fileName, //文件名称
            @RequestParam("chunkSize") Long chunkSize, //每块的大小
            @RequestParam("bucketName") String bucketName //存储桶
    ) throws RocksDBException, IOException {
        Map<String, Object> data = new HashMap<>();
        webService.saveChunk(chunk,index,chunkTotal,chunkSize,bucketName);//保存分片信息
        log.info("当前分片:"+index +" ,总分片数:"+chunkTotal+" ,文件名:"+fileName);
        if(webService.checkChunkAll()==chunkTotal){
            byte[] mergeBytes = webService.mergeChunk();//合并文件
            if(!Objects.equals(FileUtil.getMd5(mergeBytes), md5)){
                data.put("info","md5值校验不一致!");
                return new R<>(200,"请求成功",data);
            }
            webService.saveObject(mergeBytes,bucketName,fileName);//保存对象信息到rocksdb，并删除临时文件
            data.put("info:","文件上传成功");
            return new R<>(200,"请求成功",data);
        }
        data.put("index:",index);
        return new R<>(200,"请求成功",data);
    }
}
