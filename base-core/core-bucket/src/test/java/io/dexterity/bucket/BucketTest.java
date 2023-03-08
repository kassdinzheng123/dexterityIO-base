package io.dexterity.bucket;

import cn.hutool.json.JSONUtil;

import java.util.HashMap;

public class BucketTest {
    public static void main(String[] args){
        HashMap<String,String> tags = new HashMap<>();
        tags.put("级别","高");
        tags.put("用途","临时");
        System.out.println(tags);

        String result = JSONUtil.toJsonStr(tags);
        System.out.println(result);
    }
}
