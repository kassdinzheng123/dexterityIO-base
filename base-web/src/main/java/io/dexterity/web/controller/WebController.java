package io.dexterity.web.controller;

import io.dexterity.bucket.po.vo.BucketVO;
import io.dexterity.service.api.BucketApi;
import io.dexterity.service.api.StorageApi;
import io.dexterity.web.po.pojo.R;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/web")
public class WebController {
    @Autowired
    private BucketApi bucketApi;
    @Autowired
    private StorageApi storageApi;

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
}
