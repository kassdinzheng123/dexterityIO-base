package io.dexterity.utils;

import cn.hutool.core.util.IdUtil;
import jakarta.servlet.http.HttpServletRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.multipart.MultipartFile;

import java.io.*;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.text.DecimalFormat;

public class FileUtil extends cn.hutool.core.io.FileUtil {
    /**
     * 使用指定的类初始化日志对象，方便在日志输出的时候，可以打印出日志信息所属的类。
     */
    private static final Logger log = LoggerFactory.getLogger(FileUtil.class);
    /**
     * 系统临时目录
     * <br>
     * windows 包含路径分割符,但Linux不包含,
     * 在windows \\==\ 前提下,
     * 为安全起见 同意拼装 路径分割符,
     * <pre>
     *       java.io.tmpdir
     *       windows : C:\Users\xxx\AppData\Local\Temp\
     *       linux: /temp
     * </pre>
     */
    public static final String SYS_TEM_DIR = System.getProperty("java.io.tmpdir") + File.separator;
    /**
     * 定义GB的计算常量
     */
    private static final int GB = 1024 * 1024 * 1024;
    /**
     * 定义MB的计算常量
     */
    private static final int MB = 1024 * 1024;
    /**
     * 定义KB的计算常量
     */
    private static final int KB = 1024;
    /**
     * 格式化小数
     */
    private static final DecimalFormat DF = new DecimalFormat("0.00");
    /**
     * 获取文件扩展名，不带 .
     */
    public static String getExtensionName(String filename) {
        if ((filename != null) && (filename.length() > 0)) {
            int dot = filename.lastIndexOf('.');
            if ((dot > -1) && (dot < (filename.length() - 1))) {
                return filename.substring(dot + 1);
            }
        }
        return filename;
    }
    /**
     * Java文件操作 获取不带扩展名的文件名
     */
    public static String getFileNameNoEx(String filename) {
        if ((filename != null) && (filename.length() > 0)) {
            int dot = filename.lastIndexOf('.');
            if ((dot > -1) && (dot < (filename.length()))) {
                return filename.substring(0, dot);
            }
        }
        return filename;
    }
    /**
     * MultipartFile转File
     */
    public static File multipartFileToFile(MultipartFile multipartFile) {
        // 获取文件名
        String fileName = multipartFile.getOriginalFilename();
        String prefix = getFileNameNoEx(fileName);
        // 获取文件后缀
        String suffix = "." + getExtensionName(fileName);
        File newFile = null;
        FileUtil.file(SYS_TEM_DIR+"bucketTemp").mkdir(); //创建file对象(创建以file对象名称命名的文件夹)
        try {
            // IdUtil.simpleUUID():用uuid作为文件名，防止生成的临时文件重复
            File file = File.createTempFile(IdUtil.simpleUUID(), suffix,new File(SYS_TEM_DIR+"bucketTemp"));
            // MultipartFile to File
            multipartFile.transferTo(file);
            newFile = new File(SYS_TEM_DIR+"bucketTemp"+File.separator+fileName);
            file.renameTo(newFile);
        } catch (IOException e) {
            log.error(e.getMessage(), e);
        }
        return newFile;
    }
    /**
     * 文件大小转换
     */
    public static String getSize(long size) {
        String resultSize;
        if (size / GB >= 1) {
            //如果当前Byte的值大于等于1GB
            resultSize = DF.format(size / (float) GB) + "GB   ";
        } else if (size / MB >= 1) {
            //如果当前Byte的值大于等于1MB
            resultSize = DF.format(size / (float) MB) + "MB   ";
        } else if (size / KB >= 1) {
            //如果当前Byte的值大于等于1KB
            resultSize = DF.format(size / (float) KB) + "KB   ";
        } else {
            resultSize = size + "B   ";
        }
        return resultSize;
    }
    /**
     * inputStream 转 File
     */
    static File inputStreamToFile(InputStream ins, String name) throws Exception {
        File file = new File(SYS_TEM_DIR + name);
        if (file.exists()) {
            return file;
        }
        OutputStream os = new FileOutputStream(file);
        int bytesRead;
        int len = 8192;
        byte[] buffer = new byte[len];
        while ((bytesRead = ins.read(buffer, 0, len)) != -1) {
            os.write(buffer, 0, bytesRead);
        }
        os.close();
        ins.close();
        return file;
    }

    /**
     * 文件上传初始化—资源映射
     */
    public static String initialize(String rootPath,String fileName,String bucketName){
        String suffix = FileUtil.getExtensionName(fileName); //文件后缀
        String resultFileName = rootPath+File.separator+bucketName+File.separator+fileName+File.separator+"data."+suffix; //文件上传后的资源路径
        FileUtil.mkdir(rootPath+File.separator+bucketName+File.separator+fileName); //创建数据实体父目录
        return resultFileName;
    }
    /**
     * 文件类型转换，转换成中文
     */
    public static String getFileType(String type) {
        String documents = "txt pdf pps wps doc docx ppt pptx xls xlsx";
        String music = "mp3 wav wma mpa ram ra aac aif m4a";
        String video = "avi mpg mpe mpeg asf wmv mov qt rm mp4 flv m4v webm ogv ogg";
        String image = "bmp dib pcp dif wmf gif jpg tif eps psd cdr iff tga pcd mpt png jpeg";
        if (image.contains(type)) {
            return "图片";
        } else if (documents.contains(type)) {
            return "文档";
        } else if (music.contains(type)) {
            return "音乐";
        } else if (video.contains(type)) {
            return "视频";
        } else {
            return "其他";
        }
    }
    /**
     * 文件类型转换，转换成字符串
     */
    public static String getTransferFileType(String type) {
        String documents = "txt pdf pps wps doc docx ppt pptx xls xlsx";
        String music = "mp3 wav wma mpa ram ra aac aif m4a";
        String video = "avi mpg mpe mpeg asf wmv mov qt rm mp4 flv m4v webm ogv ogg";
        String image = "bmp dib pcp dif wmf gif jpg tif eps psd cdr iff tga pcd mpt png jpeg";
        if (image.contains(type)) {
            return "image";
        } else if (documents.contains(type)) {
            return "documents";
        } else if (music.contains(type)) {
            return "music";
        } else if (video.contains(type)) {
            return "video";
        } else {
            return "other";
        }
    }
    /**
     * 检查文件大小
     */
    public static void checkSize(long maxSize, long size) {
        // 1M
        int len = 1024 * 1024;
        if (size > (maxSize * len)) {
            //TODO
            System.out.println("文件超出规定大小");
        }
    }
    /**
     * 将文件转化为字节流
     */
    private static byte[] getByte(File file) {
        // 得到文件长度
        byte[] b = new byte[(int) file.length()];
        try {
            InputStream in = new FileInputStream(file);
            try {
                System.out.println(in.read(b));
            } catch (IOException e) {
                log.error(e.getMessage(), e);
            }
        } catch (FileNotFoundException e) {
            log.error(e.getMessage(), e);
            return null;
        }
        return b;
    }
    /**
     * 计算字节流的MD5值
     */
    public static String getMd5(byte[] bytes) {
        // 16进制字符
        char[] hexDigits = {'0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f'};
        try {
            MessageDigest mdTemp = MessageDigest.getInstance("MD5");
            mdTemp.update(bytes);
            byte[] md = mdTemp.digest();
            int j = md.length;
            char[] str = new char[j * 2];
            int k = 0;
            // 移位 输出字符串
            for (byte byte0 : md) {
                str[k++] = hexDigits[byte0 >>> 4 & 0xf];
                str[k++] = hexDigits[byte0 & 0xf];
            }
            return new String(str);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
        return null;
    }
    /**
     * 计算文件的MD5值
     */
    public static String getMd5(File file) {
        return getMd5(getByte(file));
    }
    /**
     * 通过MD5值判断两个文件是否相同
     */
    public static boolean check(File file1, File file2) {
        String img1Md5 = getMd5(file1);
        String img2Md5 = getMd5(file2);
        return img1Md5.equals(img2Md5);
    }
    /**
     * 直接通过MD5值判断两个文件是否相同
     */
    public static boolean check(String file1Md5, String file2Md5) {
        return file1Md5.equals(file2Md5);
    }

    public static String filenameEncoding(String filename, HttpServletRequest request) {
        // 获得请求头中的User-Agent
        String agent = request.getHeader("User-Agent");
        // 根据不同的客户端进行不同的编码
        filename = URLEncoder.encode(filename, StandardCharsets.UTF_8);
        return filename;
    }
}