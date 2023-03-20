package io.dexterity.controller.object;

import io.dexterity.entity.MetaData;
import io.dexterity.entity.Retention;
import io.dexterity.entity.xml.Delete;
import io.dexterity.entity.xml.LegalHold;
import io.dexterity.entity.xml.Tagging;
import io.dexterity.po.R;
import jakarta.servlet.http.HttpServletRequest;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpRequest;
import org.springframework.web.bind.annotation.*;

import java.util.Enumeration;

@RestController
public class ObjectBaseController {

    public MetaData getHeaders(HttpServletRequest request) {
        MetaData entity = new MetaData();
        Enumeration<String> headerNames = request.getHeaderNames();
        while (headerNames.hasMoreElements()) {
            String headerName = headerNames.nextElement();
            switch (headerName) {
                case MetaData.CACHE_CONTROL -> entity.setCacheControl(request.getHeader(headerName));
                case MetaData.CONTENT_DISPOSITION -> entity.setContentDisposition(request.getHeader(headerName));
                case MetaData.CONTENT_ENCODING -> entity.setContentEncoding(request.getHeader(headerName));
                case MetaData.CONTENT_LANGUAGE -> entity.setContentLanguage(request.getHeader(headerName));
                case MetaData.CONTENT_MD5 -> entity.setContentMD5(request.getHeader(headerName));
                case MetaData.CONTENT_TYPE -> entity.setContentType(request.getHeader(headerName));
                case MetaData.EXPIRES -> entity.setExpires(request.getHeader(headerName));
                default -> {
                    if (headerName.startsWith("dexIO-user-")){
                        entity.setUserMetadata(headerName,request.getHeader(headerName));
                    }
                }
            }
        }
        return entity;
    }

    /**
     * PUT OBJECT LegalHold
     */
    @PutMapping("/<key>?legal-hold")
    public void putLegalHold(@PathVariable String key,
                          @RequestBody LegalHold legalHold,
                          HttpServletRequest request) {

    }


    /**
     * PUT OBJECT Retention
     */
    @PutMapping("/{key}/")
    public void putRetention(@PathVariable String key,
                             @RequestBody Retention retention,
                             HttpServletRequest request){

    }

    /**
     * PUT OBJECT TAGGING
     */
    @PutMapping("/{key}?tagging")
    public void putTagging(@PathVariable String key,
                           @RequestBody Tagging tagging,
                           HttpServletRequest request) {

    }


    /**
     * GET OR HEAD OBJECT
     */
    @GetMapping("/{Key}")
    public R<Byte[]> getObject(@PathVariable String key,
                               @RequestHeader("If-Match") String ifMatch,
                               @RequestHeader("If-Modified-Since") String ifModifiedSince,
                               @RequestHeader("If-None-Match") String ifNoneMatch,
                               @RequestHeader("If-Unmodified-Since") String ifUnmodifiedSince,
                               @RequestHeader("Range") String range,
                               @RequestHeader("Encryption-password") String sseCustomerKeyMD5,
                               HttpRequest httpRequest) {

        if (httpRequest.getMethod().equals(HttpMethod.HEAD)){

        }else{

        }
        //TODO
        return null;
    }

    /**
     * GET OBJECT LegalHold
     */
    @GetMapping("/{key}?legal-hold")
    public void putLegalHold(@PathVariable String key,
                          HttpServletRequest request) {

    }

    /**
     * GUT OBJECT TAGGING
     */
    @GetMapping("/{key}?tagging")
    public R<Tagging> putTagging(@PathVariable String key,
                           HttpServletRequest request) {
        //TODO
        return null;
    }

    /**
     * GET OBJECT Retention
     */
    @PutMapping("/{key}/")
    public void getRetention(@PathVariable String key,
                             HttpServletRequest request){

    }


    /**
     * Delete OBJECT
     */
    @DeleteMapping("/{Key}")
    public R<byte[]> deleteObject(@PathVariable String key) {
        //TODO
        return null;
    }


    /**
     * Delete OBJECTS
     */
    @DeleteMapping("/?delete")
    public R<byte[]> deleteObjects(@PathVariable String key, Delete xml) {
        //TODO
        return null;
    }









}
