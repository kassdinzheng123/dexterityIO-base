package io.dexterity.web;

import io.dexterity.service.api.StorageApi;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

@SpringBootTest
public class WebTest {
    @Autowired
    private StorageApi storageApi;
    @Test
    public void test(){
        System.out.println(storageApi.cfAll());
    }
}
