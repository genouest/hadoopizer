package org.genouest.hadoopizer.output;

import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.RecordWriter;

public abstract class HadoopizerRecordWriter<K, V> extends RecordWriter<K, V> {

    /**
     * TODO document
     * 
     * @param out
     * @param conf
     */
    protected void writeHeader(DataOutputStream out, Configuration conf, Path headerTempFile) {

        if (headerTempFile != null) {
            try {
                FileSystem fs = headerTempFile.getFileSystem(conf);
            
                if (fs.exists(headerTempFile)) {
                    FSDataInputStream in = fs.open(headerTempFile);
                    
                    byte[] buffer = new byte[8192];
                    int bytesRead;
                    while ((bytesRead = in.read(buffer)) > 0) {
                        out.write(buffer, 0, bytesRead);
                    }
                    
                    in.close();
                }
            } catch (IOException e) {
                System.err.println("Failed to copy output header from " + headerTempFile + " to output file");
                e.printStackTrace();
                System.exit(1);
            }
        }
    }
}