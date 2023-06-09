package io.dexterity.controller;

import io.dexterity.entity.constants.MetaDataConstants;
import io.dexterity.po.R;
import io.dexterity.po.vo.BucketVO;
import io.dexterity.service.BucketService;
import io.dexterity.service.WebService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import lombok.extern.slf4j.Slf4j;
import org.rocksdb.RocksDBException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
@Slf4j
@RestController
@RequestMapping("/web")
@Tag(name = "后端存储服务",description = "暂无描述")
public class WebController {
    @Autowired
    private WebService webService;

    @Autowired
    private BucketService bucketService;

    @Operation(summary = "查询存储桶列表", description = "从数据库查询创建的存储桶列表")
    @GetMapping("/bucket")
    public R<?> listBucket(){
        return new R<>(200,"请求成功",bucketService.listBucket());
    }

    @Operation(summary = "创建存储桶", description = "创建一个新的存储桶")
    @PostMapping("/bucket")
    public R<?> createBucket(@RequestBody BucketVO bucket) throws RocksDBException {
        return new R<>(200,"请求成功",bucketService.createBucket(bucket));
    }

    @Operation(summary = "删除存储桶", description = "根据前端传来的存储桶id删除存储桶")
    @DeleteMapping("/bucket")
    public R<?> deleteBucket(@RequestParam("bucketName")String bucketName) throws RocksDBException {
        return new R<>(200,"请求成功",bucketService.deleteBucket(bucketName));
    }

    @Operation(summary = "更新存储桶状态", description = "通过前端传来的值进行更改存储桶状态")
    @PutMapping("/bucket")
    public R<?> updateBucketStatus(@RequestParam("bucketId")String bucketId,
                                  @RequestParam("status")Integer status){
        return new R<>(200,"请求成功",bucketService.updateStatusBucket(bucketId,status));
    }

    @Operation(summary = "上传对象", description = "上传对象到指定存储桶")
    @PostMapping("/object")
    public R<?> uploadToBucket(
            @RequestParam("chunk") MultipartFile chunk, //块的数据
            @RequestParam("crypto") String crypto, //文件的sha256值
            @RequestParam("chunkCrypto") String chunkCrypto, //块的sha256值
            @RequestParam("index") Integer index, //块的序号
            @RequestParam("chunkTotal") Integer chunkTotal, //块的总数
            @RequestParam("fileSize") Long fileSize, //文件大小
            @RequestParam("fileName") String fileName, //文件名称
            @RequestParam("chunkSize") Long chunkSize, //每块的大小
            @RequestParam("bucketName") String bucketName //存储桶
    ) throws RocksDBException, IOException {
        Map<String, Object> data = new HashMap<>();
        List<String> dup = List.of("fileName", "fileSize", "chunk","chunkTotal","bucketName");
        List<String> unDup = List.of(MetaDataConstants.LMDB_METADATA_KEY);
        // 保存分片信息
        if(webService.saveChunk(chunk,index,chunkTotal, chunkSize,crypto,
                bucketName,fileName, fileSize,chunkCrypto,dup,unDup)==1){
            log.info("当前分片:"+index +" ,总分片数:"+chunkTotal+" ,文件名:"+fileName);
            data.put("sha256",chunkCrypto);
            return new R<>(200,"分片上传成功",data);
        }else{
            return new R<>(500,"分片校验失败");
        }


//        if(webService.checkChunkAll()==chunkTotal){
//            byte[] mergeBytes = webService.mergeChunk(); // 合并文件
//            log.info("总校验和："+FileUtil.getMd5(mergeBytes));
//            if(!Objects.equals(FileUtil.getMd5(mergeBytes), crypto)){
//                data.put("info","md5值校验不一致!");
//                return new R<>(200,"请求成功",data);
//            }
//            webService.saveObject(mergeBytes,bucketName,fileName,crypto,fileSize); // 保存对象信息到rocksdb，并删除临时文件
//            data.put("info:","文件上传成功");
//            return new R<>(200,"请求成功",data);
//        }
    }

    @Operation(summary = "查询对象列表", description = "查询某存储桶中的对象列表")
    @GetMapping("/object")
    public R<?> getObjByBucket(
            @RequestParam("bucketName") String bucketName
    ) throws RocksDBException {
        return new R<>(200,"请求成功",webService.getAllObj(bucketName));
    }

    @Operation(summary = "检查对象是否上传", description = "根据前端传来的MD5查询某存储桶的某对象是否已上传")
    @GetMapping("/object/check")
    public R<?> checkObject(@RequestParam("crypto") String crypto,
                            @RequestParam("bucketName")String bucketName){
        log.info("对象sha256:"+crypto);
        // 首先检查对象是否存在
        Boolean isUploaded = webService.findObjByCrypto(crypto,bucketName);

        // 定义一个返回值集合
        Map<String, Object> data = new HashMap<>();
        data.put("isUploaded",isUploaded);

        // 有，则执行秒传
        if(isUploaded){
            data.put("info","执行秒传");
            return new R<>(200,"请求成功",data);
        }
        // 没有，则查询是否存在分片信息并返回给前端（交给前端判断）
        // 若存在分片，则执行断点续传
        // 若不存在分片，则按照正常上传
        //TODO 先默认chunkList为空
        List<Integer> chunkList = webService.findChunkListByMD5(crypto);
        if(chunkList.size()!=0){
            data.put("chunkList",chunkList);
            data.put("info","执行断点续传");
            return  new R<>(200,"请求成功",data);
        }
        data.put("chunkList",chunkList);
        data.put("info","执行正常上传");
        return  new R<>(200,"请求成功",data);
    }
    @Operation(summary = "删除对象", description = "从指定存储桶中删除指定对象")
    @DeleteMapping("/object")
    public R<?> deleteObjByBucket(
            @RequestParam("bucketName") String bucketName,
            @RequestParam("fileName") String fileName
    ) throws RocksDBException {
        return new R<>(200,"请求成功",webService.deleteObj(bucketName,fileName));
    }
}
