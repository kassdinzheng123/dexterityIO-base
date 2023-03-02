package io.dexterity.controller;

import io.dexterity.api.BucketApi;
import io.dexterity.pojo.po.R;
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
    public R<?> test(){
        int result = bucketApi.deleteBucket();
        return new R<>(200,"请求成功",result);
    }
}
