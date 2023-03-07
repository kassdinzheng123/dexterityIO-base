package io.dexterity.web.controller;

import io.dexterity.service.api.BucketApi;
import io.dexterity.web.po.pojo.R;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/web")
public class WebController {
    @Autowired
    private BucketApi bucketApi;
    @GetMapping("/bucket")
    public R<?> listBucket(){
        return new R<>(200,"请求成功",bucketApi.listBucket());
    }

    @GetMapping("/test")
    public R<?> test1(){
        return new R<>(200,"请求成功");
    }
}
