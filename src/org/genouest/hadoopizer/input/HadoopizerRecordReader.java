package org.genouest.hadoopizer.input;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.RecordReader;

public abstract class HadoopizerRecordReader<K, V> extends RecordReader<K, V> {

    private Path headerTempFile;
    private FSDataOutputStream headerOut;

    /**
     * Create a HadoopizerRecordReader
     * Implementations should read input files and write header lines in given headerTempFile if it doesn't already exist
     * 
     * @param headerTempFile File where header content should be saved  
     */
    public HadoopizerRecordReader(Path headerTempFile, Configuration conf) {
        
        this.headerTempFile = headerTempFile;
        
        if (headerTempFile != null) {
            try {
                FileSystem fs = headerTempFile.getFileSystem(conf);
                
                if (!fs.exists(headerTempFile)) {
                    headerOut = fs.create(headerTempFile, false);
                }
            } catch (IOException e) {
                headerOut = null;
            }
        }
    }
    
    /**
     * Get the path of the temporary header file
     * 
     * @return the path of the temporary header file
     */
    public Path getHeaderTempFile() {
        
        return headerTempFile;
    }
    
    /**
     * Write a header line to a temporary file
     * This file can then be read later and prepended to output files
     * 
     * @param line a header line to write
     * @return true if the line was written, false otherwise
     * @throws IOException
     */
    public boolean writeHeaderLine(String line) throws IOException {
        if (headerOut == null)
            return false;

        headerOut.write(line.getBytes());
        headerOut.write("\n".getBytes());
        
        return true;
    }

    /**
     * Call this when you have finished writing header lines to the temporary file
     * This ensures that the temporary file is correctly flushed before being read by some other code (usually a HadoopizerOutputFormat)
     * 
     * @throws IOException
     */
    protected void headerFinished() throws IOException {
        
        if (headerOut != null) {
            headerOut.close();
            headerOut = null;
        }
    }

    @Override
    public void close() throws IOException {

        if (headerOut != null)
            headerOut.close();
    }
}
