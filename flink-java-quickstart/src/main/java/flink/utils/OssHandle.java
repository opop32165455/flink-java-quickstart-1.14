package flink.utils;

import java.io.File;
import java.io.InputStream;

/**
 * @author zhangxuecheng4441
 * @date 2022/8/12/012 14:51
 */

public interface OssHandle {
    Object LOCK = new Object();

    /**
     * 文件上传
     *
     * @param inputStream   inputStream
     * @param inputFileName inputFileName
     * @return 上传path
     */
    String upload(InputStream inputStream, String inputFileName);

    /**
     * 文件读取
     *
     * @param filePath inputStream
     * @return 文件下载
     */
    File download(String filePath);

    /**
     * 下载文件返回流
     *
     * @param filePath filePath
     * @return InputStream
     */
    InputStream downloadStream(String filePath);
    /***
     * 创建唯一临时文件名称
     *
     * @return 创建不重复名称
     */
    static String createTmpOnlyName() {
        synchronized (LOCK) {
            return "/tmp/" + System.currentTimeMillis();
        }
    }
}
