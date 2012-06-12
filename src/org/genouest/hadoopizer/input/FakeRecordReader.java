package org.genouest.hadoopizer.input;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.ObjectWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.util.LineReader;
import org.genouest.hadoopizer.io.ObjectWritableComparable;

/**
 * Inspired by org.apache.hadoop.mapreduce.lib.input.LineRecordReader
 */
public class FakeRecordReader extends HadoopizerRecordReader {

    private long start;
    private long end;
    private long pos;

    private LineReader lineReader;
    private String nextLine = "";

    private Text recordKey = new Text();
    private LongWritable recordValue = new LongWritable();
    Configuration conf;

    public FakeRecordReader(Path headerTempFile, Configuration conf) {
        
        super(headerTempFile, conf);
    }
    
    @Override
    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {

        FileSplit fileSplit = (FileSplit) split;
        conf = context.getConfiguration();

        start = fileSplit.getStart();
        end = start + fileSplit.getLength();
        
        Path path = fileSplit.getPath();
        CompressionCodecFactory compressionCodecs = new CompressionCodecFactory(conf);
        CompressionCodec codec = compressionCodecs.getCodec(path);
        
        FileSystem fs = path.getFileSystem(conf);
        FSDataInputStream fsin = fs.open(path);
        
        if (codec != null) {
            // Input file is compressed: it is not splitted => no need to seek
            lineReader = new LineReader(codec.createInputStream(fsin), conf);
            end = Long.MAX_VALUE;
        }
        else {
            lineReader = new LineReader(fsin, conf);
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
        
        recordKey.set(nextLine);
        recordValue.set(nextLine.length());
        
        return true;
    }

    @Override
    public ObjectWritableComparable getCurrentKey() throws IOException, InterruptedException {

        ObjectWritableComparable key = new ObjectWritableComparable();
        key.set("", recordKey);
        return key; // FIXME no key ?
    }
    
    @Override
    public ObjectWritableComparable getCurrentKey(String id) throws IOException, InterruptedException {

        ObjectWritableComparable key = new ObjectWritableComparable();
        key.set(id, recordKey);
        return key;
    }

    @Override
    public ObjectWritable getCurrentValue() throws IOException, InterruptedException {

        return new ObjectWritable(recordValue);
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