package org.genouest.hadoopizer.input;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ObjectWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.util.LineReader;
import org.genouest.hadoopizer.io.TaggedObjectWritable;
import org.genouest.hadoopizer.io.ObjectWritableComparable;

/**
 * Inspired by org.apache.hadoop.mapreduce.lib.input.LineRecordReader
 */
public class FastaRecordReader extends HadoopizerRecordReader {

    private long start;
    private long end;
    private long pos;

    private LineReader lineReader;
    private String nextLine = "";
    private boolean reachedEof = false;
    Configuration conf;

    private Text recordKey = new Text();
    private Text recordValue = new Text();

    public FastaRecordReader(Path headerTempFile, Configuration conf) {
        
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
        
        // Seek to the next fasta header (if we're not already positionned on a fasta header)
        readUntilNextRecord();
        
        headerFinished(); // No header in fasta
        
        trackOrigin(conf, path);
    }

    /**
     * Reads the fasta file until the next fasta header
     * When a fasta header is encountered, the corresponding line is placed in nextLine variable
     * 
     * @return the full fasta content encountered before the next fasta header, without line breaks
     * @throws IOException 
     */
    public String readUntilNextRecord() throws IOException {
        
        String sequence = "";
        Text newLine = new Text("");
        boolean foundHeader = false;
        boolean noDataLeft = false;
        int read;

        while (!foundHeader && !noDataLeft) {
            read = lineReader.readLine(newLine);
            pos += read;
            noDataLeft = (read == 0);
            nextLine = newLine.toString();

            if (nextLine.startsWith(">"))
                foundHeader = true;
            else if (!nextLine.isEmpty())
                sequence += nextLine;
        }
        
        reachedEof = noDataLeft;

        return sequence;
    }
    
    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        
        if ((pos >= end) || reachedEof) // Reached the end of split
            return false;
        
        if (nextLine.startsWith(">")) {
            
            recordKey.set(nextLine.substring(1));
            recordValue.set(readUntilNextRecord());
            
            return true;
        }
        else {
            // Means we didn't call readUntilNextRecord before which is not supposed to happen
            throw new IOException("Failed to parse Fasta file: couldn't find header in '" + nextLine + "'");
        }
    }

    @Override
    public ObjectWritableComparable getCurrentKey() throws IOException, InterruptedException {
        
        ObjectWritableComparable key = new ObjectWritableComparable();
        key.set("", recordKey);
        return key;
    }
    
    @Override
    public ObjectWritableComparable getCurrentKey(String id) throws IOException, InterruptedException {

        ObjectWritableComparable key = new ObjectWritableComparable();
        key.set(id, recordKey);
        return key;
    }

    @Override
    public ObjectWritable getCurrentValue() throws IOException, InterruptedException {

        TaggedObjectWritable data = new TaggedObjectWritable(getInputId(), new ObjectWritable(recordValue));
        return new ObjectWritable(data);
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
