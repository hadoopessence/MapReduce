package org.techmytalk.mapreducesample;

import java.io.IOException;
import java.net.URI;

import junit.framework.Assert;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

public class SampleFileSystem {
    Configuration conf = null;

    public SampleFileSystem() {
        conf = new Configuration();
        //conf.se
    }
    
    
    public void createDir(String uri) throws IOException{
        URI sourceUri = URI.create(uri);
        FileSystem file = FileSystem.get(sourceUri,conf);
        System.out.println("Create Dir:"+file.mkdirs(new Path(sourceUri)));
        
    }
    
    public void getFileStatus(String strURI) throws IOException{
        
        URI uri=URI.create(strURI);
        FileSystem fileSystem=FileSystem.get(uri,conf);
        FileStatus fileStatus=fileSystem.getFileStatus(new Path(uri));
        System.out.println("AccessTime:"+fileStatus.getAccessTime());
        System.out.println("AccessTime:"+fileStatus.getLen());
        System.out.println("AccessTime:"+fileStatus.getModificationTime());
        System.out.println("AccessTime:"+fileStatus.getPath());
    }

    public void appendFile(String sourceURI,String destURI) throws IOException{
            URI sourceUri = URI.create(sourceURI);
            URI destUri = URI.create(destURI);
            FileSystem file = FileSystem.get(sourceUri, conf);
            file.append(new Path(destUri));
            readFileContent(sourceURI);
            
            
            //InputStream in = new BufferedInputStream(new FileInputStream(localSrc));
            //Configuration conf = new Configuration();
            //FileSystem fs = FileSystem.get(URI.create(dst), conf); OutputStream out = fs.create(new Path(dst), new Progressable() {
           /// public void progress() {
            //System.out.print("."); }
            //});
            //IOUtils.copyBytes(in, out, 4096, true); }
        

        
        
    }
    public void readFileContent(String uriStr) throws IOException {
        URI uri = URI.create(uriStr);

        FileSystem file = FileSystem.get(uri, conf);
        FSDataInputStream in = null;

        in = file.open(new Path(uri));


        IOUtils.copyBytes(in, System.out, conf);

    }
    
    public void readFile(String uriStr) throws IOException {
        URI uri = URI.create(uriStr);

        FileSystem file = FileSystem.get(uri, conf);
        FSDataInputStream in = null;

        in = file.open(new Path(uri));
        byte[] btbuffer = new byte[5];

        // IOUtils.readFully(in, btbuffer, 0, 1);//Reads len bytes in a loop.
        in.read(btbuffer, 0, 5);
        System.out.println("PRINT from 0th position=>" + new String(btbuffer));
        in.readFully(btbuffer);
        System.out.println("PRINT from 5th position=>" + new String(btbuffer));//
        in.seek(0); // sent to start position
        Assert.assertEquals(0, in.getPos());
        in.read(btbuffer, 0, 5);
        System.out.println("PRINT first 5 charcter=>" + new String(btbuffer));
        
        in.read(5,btbuffer, 0, 5);
        System.out.println("PRINT from 5th position=>" + new String(btbuffer));

    }
    public static void main(String[] args) throws Exception {
        SampleFileSystem fileSystemSample = new SampleFileSystem();
        String uri = "hdfs://localhost:9000/user/joe/TestFile.txt";
        
        String mkDiruri = "hdfs://localhost:9000/user/joe/test/";
        
        fileSystemSample.createDir(mkDiruri);
        fileSystemSample.getFileStatus(uri);
        fileSystemSample.readFileContent(uri);
        
        //fileSystemSample.appendFile(uri,uri);
        //fileSystemSample.sampleFileSystem(uri);
    }
}
