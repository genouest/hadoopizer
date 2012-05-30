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
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.util.LineReader;
import org.genouest.hadoopizer.Hadoopizer;

/**
 * Inspired by org.apache.hadoop.mapreduce.lib.input.LineRecordReader
 */
public class FastaRecordReader extends RecordReader<Text, Text> {

    private long start;
    private long end;
    private long pos;

    private LineReader lineReader;
    private String nextLine = "";

    private Text recordKey = new Text();
    private Text recordValue = new Text();
    
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
            end = Long.MAX_VALUE; // TODO see if it works with splittable compressed files (lzo?)
            // TODO teste compressed fasta
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
        
        // Seek to the next fasta header (if we're not already positionned on a fasta header)
        readUntilNextRecord();
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

        while (!foundHeader && !noDataLeft) { // FIXME check what happens with empty lines
            pos += lineReader.readLine(newLine);
            nextLine = newLine.toString();
            noDataLeft = newLine.getLength() <= 0;

            if (!nextLine.startsWith(">"))
                sequence += nextLine;
            else
                foundHeader = true;
        }

        return sequence;
    }
    
    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        
        if (pos >= end) // Reached the end of split
            return false;
        
        if (nextLine.startsWith(">")) {
            
            recordKey.set(nextLine.substring(1));
            recordValue.set(readUntilNextRecord());
            
            return true;
        }
        else if (nextLine.isEmpty() && (pos > start)) {
            return false;
        }
        else {
            // Means we didn't call readUntilNextRecord before which is not supposed to happen
            throw new IOException("Failed to parse Fasta file: couldn't find header in '" + nextLine + "'");
        }
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
