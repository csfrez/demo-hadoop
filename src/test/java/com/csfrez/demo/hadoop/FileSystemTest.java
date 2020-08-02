package com.csfrez.demo.hadoop;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.junit.Test;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class FileSystemTest {


    @Test
    public void getFileSystem1() throws IOException {
        Configuration configuration = new Configuration();
        configuration.set("fs.defaultFS", "hdfs://node-master:9000");
        FileSystem fileSystem = FileSystem.get(configuration);
        System.out.println("+++++++++++++++++++++");
        System.out.println(fileSystem.toString());
        System.out.println("+++++++++++++++++++++");
    }

    @Test
    public void getFileSystem2() throws IOException, URISyntaxException {
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://node-master:9000"), new Configuration());
        System.out.println("+++++++++++++++++++++");
        System.out.println(fileSystem.toString());
        System.out.println("+++++++++++++++++++++");
    }

    @Test
    public void getFileSystem3() throws IOException {
        Configuration configuration = new Configuration();
        configuration.set("fs.defaultFS", "hdfs://node-master:9000");
        FileSystem fileSystem = FileSystem.newInstance(configuration);
        System.out.println("+++++++++++++++++++++");
        System.out.println(fileSystem.toString());
        System.out.println("+++++++++++++++++++++");
    }

    @Test
    public void getFileSystem4() throws IOException, URISyntaxException {
        FileSystem fileSystem = FileSystem.newInstance(new URI("hdfs://node-master:9000"), new Configuration());
        System.out.println("+++++++++++++++++++++");
        System.out.println(fileSystem.toString());
        System.out.println("+++++++++++++++++++++");
    }

    @Test
    public void listMyFiles() throws Exception{
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://node-master:9000"), new Configuration());
        RemoteIterator<LocatedFileStatus> locatedFileStatusRemoteIterator = fileSystem.listFiles(new Path("/"), true);
        while (locatedFileStatusRemoteIterator.hasNext()){
            LocatedFileStatus next = locatedFileStatusRemoteIterator.next();
            BlockLocation[] blockLocations = next.getBlockLocations();
            System.out.println(next.getPath().toString() + "==>" + blockLocations.length);

        }
        fileSystem.close();
    }

    @Test
    public void mkdirs() throws Exception{
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://node-master:9000"), new Configuration());
        //fileSystem.mkdirs(new Path("/dir2/dir22/dir222"));
        fileSystem.create(new Path("/dir2/dir22/dir222/b.xml"));
        fileSystem.close();
    }

    @Test
    public void getFileToLocal1() throws Exception{
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://node-master:9000"), new Configuration());
        FSDataInputStream inputStream = fileSystem.open(new Path("/a.txt"));
        FileOutputStream outputStream = new FileOutputStream(new File("D:\\Temp\\a.txt"));
        IOUtils.copy(inputStream, outputStream);
        IOUtils.closeQuietly(inputStream);
        IOUtils.closeQuietly(outputStream);
        fileSystem.close();
    }

    @Test
    public void getFileToLocal2() throws Exception{
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://node-master:9000"), new Configuration());
        fileSystem.copyToLocalFile(new Path("/a.txt"), new Path("D:\\Temp\\b.txt"));
        fileSystem.close();
    }

    @Test
    public void uploadFile() throws Exception{
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://node-master:9000"), new Configuration());
        fileSystem.copyFromLocalFile(new Path("D:\\Temp\\b.txt"), new Path("/b.txt"));
        fileSystem.close();
    }

    @Test
    public void mergeFile() throws Exception{
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://node-master:9000"), new Configuration());
        FSDataOutputStream outputStream = fileSystem.create(new Path("/big.txt"));

        LocalFileSystem localFileSystem = FileSystem.getLocal(new Configuration());
        FileStatus[] fileStatuses = localFileSystem.listStatus(new Path("D:\\Temp\\input"));
        for(FileStatus fileStatus : fileStatuses){
            FSDataInputStream inputStream = localFileSystem.open(fileStatus.getPath());
            IOUtils.copy(inputStream, outputStream);
            IOUtils.closeQuietly(inputStream);
        }
        IOUtils.closeQuietly(outputStream);
        localFileSystem.close();
        fileSystem.close();
    }
}
