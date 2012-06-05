package org.genouest.hadoopizer.input;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.util.LineReader;

/**
 * Inspired by org.apache.hadoop.mapreduce.lib.input.LineRecordReader
 */
public class SAMRecordReader extends HadoopizerRecordReader<Text, Text> {

    private long start;
    private long end;
    private long pos;

    private LineReader lineReader;
    private String nextLine = "";

    private Text recordKey = new Text();
    private Text recordValue = new Text();

    public SAMRecordReader(Path headerTempFile, Configuration conf) {
        
        super(headerTempFile, conf);
    }
    
    @Override
    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {

        FileSplit fileSplit = (FileSplit) split;
        Configuration job = context.getConfiguration();

        start = fileSplit.getStart();
        end = start + fileSplit.getLength();
        
        Path path = fileSplit.getPath();
        CompressionCodecFactory compressionCodecs = new CompressionCodecFactory(job);
        CompressionCodec codec = compressionCodecs.getCodec(path);
        
        FileSystem fs = path.getFileSystem(job);
        FSDataInputStream fsin = fs.open(path);
        
        if (codec != null) {
            // Input file is compressed: it is not splitted => no need to seek
            lineReader = new LineReader(codec.createInputStream(fsin), job);
            end = Long.MAX_VALUE;
        }
        else {
            lineReader = new LineReader(fsin, job);
            if (start != 0) {
                --start;
                fsin.seek(start);
            }
        }
        
        pos = start;
        
        if (start != 0 && codec == null) { // Not the beginning of the whole file (impossible if compressed as there will only be 1 split)
            // Not at the beginning of the file, throw away the first (probably incomplete) line
            Text tmp = new Text("");
            pos += lineReader.readLine(tmp);
        }
    }
    
    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        
        if (pos >= end) // Reached the end of split
            return false;

        boolean foundRecord = false;
        
        while (!foundRecord) {
            Text newLine = new Text("");
            int read = lineReader.readLine(newLine);
            
            if (read == 0)
                return false;
            
            pos += read;
            nextLine = newLine.toString();
            
            if (!nextLine.startsWith("@")) {
                if (newLine.getLength() > 0) {
                    headerFinished();
                    foundRecord = true;
                }
            }
            else {
                // Found a SAM header, write it to a temp file if needed
                writeHeaderLine(nextLine);
            }
        }
        
        int limitHeader = nextLine.indexOf("\t");
        
        recordKey.set(nextLine.substring(0, limitHeader));
        recordValue.set(nextLine.substring(limitHeader+1));
        
        return true;
    }

    @Override
    public Text getCurrentKey() throws IOException, InterruptedException {

        return recordKey;
    }

    @Override
    public Text getCurrentValue() throws IOException, InterruptedException {

        return recordValue;
    }

    @Override
    public void close() throws IOException {

        super.close();
        
        if (lineReader != null)
            lineReader.close();
    }

    @Override
    public float getProgress() throws IOException {

        if (start == end) {
            return (float) 0;
        } else {
            return Math.min((float) 1.0, (pos - start) / (float)(end - start));
        } 
    }
}
