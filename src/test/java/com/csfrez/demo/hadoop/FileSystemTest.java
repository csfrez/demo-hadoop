package com.csfrez.demo.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.junit.Test;

import java.io.IOException;

public class FileSystemTest {


    @Test
    public void getFileSystem1() throws IOException {
        Configuration configuration = new Configuration();
        configuration.set("fs.defaultFS", "hdfs://node01:8029");
        FileSystem fileSystem = FileSystem.get(configuration);
        System.out.println("+++++++++++++++++++++");
        System.out.println(fileSystem.toString());
        System.out.println("+++++++++++++++++++++");
    }
}
