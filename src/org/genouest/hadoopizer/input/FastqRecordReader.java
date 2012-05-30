package org.genouest.hadoopizer.input;

import java.io.IOException;
import java.util.ArrayList;

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
public class FastqRecordReader extends RecordReader<Text, Text> {

    private long start;
    private long end;
    private long pos;

    private LineReader lineReader;
    private ArrayList<String> currentRecord = new ArrayList<String>();

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
            shiftFastQRecord();
        }
    }

    /**
     * Reads 4 lines (=possibly a record) from the fastq file
     * 
     * @throws IOException
     */
    public void findPotentialFastQRecord() throws IOException {
        currentRecord.clear();
        
        Text newLine = new Text("");
        pos += lineReader.readLine(newLine);
        if (newLine != null && (newLine.getLength() > 0))
            currentRecord.add(newLine.toString());
        
        pos += lineReader.readLine(newLine);
        if (newLine != null && (newLine.getLength() > 0))
            currentRecord.add(newLine.toString());
        
        pos += lineReader.readLine(newLine);
        if (newLine != null && (newLine.getLength() > 0))
            currentRecord.add(newLine.toString());
        
        pos += lineReader.readLine(newLine);
        if (newLine != null && (newLine.getLength() > 0))
            currentRecord.add(newLine.toString());
    }

    /**
     * Shifts the list fastq record of 1 line in the fastq file
     * 
     * @return a list of 4 strings from the fastq file
     * @throws IOException 
     */
    public void shiftFastQRecord() throws IOException {
        if (!currentRecord.isEmpty())
            currentRecord.remove(0);

        Text newLine = new Text("");
        pos += lineReader.readLine(newLine);
        if (newLine != null && (newLine.getLength() > 0))
            currentRecord.add(newLine.toString());
    }
    
    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        
        if (pos >= end) // Reached the end of split
            return false;
        
        findPotentialFastQRecord();
        
        if (currentRecord.size() != 4)
            return false;
        
        int tries = 0;
        while (tries < 4) { // 4 lines in a record
            if (currentRecord.get(0).startsWith("@") && currentRecord.get(2).startsWith("+") && (currentRecord.get(1).length() == currentRecord.get(3).length())) {
                // This looks like a good FastQ record
                // Construct the string
                String record = currentRecord.get(1) + "\n" + currentRecord.get(2) + "\n" + currentRecord.get(3);
                
                recordKey.set(currentRecord.get(0).substring(1));
                recordValue.set(record);
                
                return true;
            }
            else {
                shiftFastQRecord();
                
                if (currentRecord.size() != 4) {
                    Hadoopizer.logger.info("Reached the end of fastq file while shifting lines");
                    return false;
                }
                
                tries++;
            }
        }
        
        // Error parsing fastq if we get there
        throw new IOException("Failed to parse FastQ file");
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
