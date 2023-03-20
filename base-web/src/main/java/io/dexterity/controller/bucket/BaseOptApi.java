package io.dexterity.controller.bucket;

import io.dexterity.entity.ListObjectsRequest;
import io.dexterity.entity.ListObjectsResponse;
import org.springframework.http.HttpRequest;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class BaseOptApi {


    /**
     * 实现需要使用公共请求头部时
     * 参见类 PublicHeaders
     */

    //TODO 权限检验
    @PutMapping("/")
    public ResponseEntity<?> putBucket(HttpRequest request){
        //TODO 读取访问控制列表相关头部
        //TODO 权限操作
        //TODO bucket插入数据库
        return null;
    }

    @RequestMapping(value = "/",method = {RequestMethod.GET})
    public ResponseEntity<ListObjectsResponse> ListObjects(ListObjectsRequest listObjectsRequest,HttpRequest request){
        //TODO 通过给出的查询条件查询Objects
        return null;
    }

    @RequestMapping(value = "/",method = {RequestMethod.HEAD})
    public ResponseEntity<?> headBucket(HttpRequest request){
        //TODO 查询存储桶是否存在
        return null;
    }

    @PutMapping("/")
    public ResponseEntity<?> deleteBucket(HttpRequest request){
        //TODO 读取访问控制列表相关头部
        //TODO 权限操作
        //TODO bucket插入数据库
        return null;
    }


}
