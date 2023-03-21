package io.dexterity.controller.bucket;

import io.dexterity.entity.BucketInfo;
import io.dexterity.entity.LifeCycle;
import io.dexterity.entity.constants.BucketLifeCycleConstants;
import io.dexterity.entity.constants.BucketServerSideEncryption;
import io.dexterity.entity.exchange.ListObjectsRequest;
import io.dexterity.entity.exchange.ListObjectsResponse;
import io.dexterity.entity.header.PublicRequestHeader;
import io.dexterity.exception.UnexpectedRequestBodyException;
import io.dexterity.util.RequestParseUtil;
import jakarta.servlet.http.HttpServletRequest;
import org.apache.commons.lang3.StringUtils;
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
    public ResponseEntity<?> putBucket(HttpServletRequest request) throws UnexpectedRequestBodyException {
        //TODO 读取访问控制列表相关头部
        //TODO 权限操作
        //TODO bucket插入数据库
        PublicRequestHeader publicRequestHeader = RequestParseUtil.readFromHttpHeaders(request);

        BucketInfo bucketInfo = new BucketInfo();
        //TODO @ZHAO ACL
        bucketInfo.setAcl("test-acl");
        //TODO @ZHAO CORS
        bucketInfo.setCors("test-cors");

        bucketInfo.setLifecycle(new LifeCycle(BucketLifeCycleConstants.CLOSE,"-1"));

        bucketInfo.setEncryption(BucketServerSideEncryption.DEFAULT);

        String first = request.getHeader("DexIO-S-Bucket-Max-Reader");
        if (!StringUtils.isEmpty(first)){
            try{
                Integer maxReader = Integer.parseInt(first);
                bucketInfo.setMaxReader(String.valueOf(maxReader));
            }catch (Exception e){
                throw new IllegalArgumentException("Max Reader Max Be Integer");
            }
        }

        String name = request.getHeader("Host");
        bucketInfo.setName(name);


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
