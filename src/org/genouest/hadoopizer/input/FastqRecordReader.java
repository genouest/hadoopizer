package org.genouest.hadoopizer.input;

import java.io.IOException;
import java.util.ArrayList;

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
import org.genouest.hadoopizer.Hadoopizer;
import org.genouest.hadoopizer.io.TaggedObjectWritable;
import org.genouest.hadoopizer.io.ObjectWritableComparable;

/**
 * Inspired by org.apache.hadoop.mapreduce.lib.input.LineRecordReader
 */
public class FastqRecordReader extends HadoopizerRecordReader {

    private long start;
    private long end;
    private long pos;
    
    private boolean reachedEof = false;

    private LineReader lineReader;
    private ArrayList<String> currentRecord = new ArrayList<String>();
    Configuration conf;

    private Text recordKey = new Text();
    private Text recordValue = new Text();

    public FastqRecordReader(Path headerTempFile, Configuration conf) {
        
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
            shiftFastQRecord();
        }
        
        headerFinished(); // No header in fastq
        
        trackOrigin(conf, path);
    }

    /**
     * Reads 4 lines (=possibly a record) from the fastq file
     * 
     * @throws IOException
     */
    public void findPotentialFastQRecord() throws IOException {
        currentRecord.clear();
        
        Text newLine = new Text("");
        
        int read;
        int foundLines = 0;
        
        while (foundLines < 4) {
            read = lineReader.readLine(newLine);
            if (read > 0) {
                pos += read;
                if (newLine.getLength() > 0) {
                    currentRecord.add(newLine.toString());
                    foundLines++;
                }
            }
            else if (read == 0) {
                reachedEof = true;
                break;
            }
        }
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
        
        int read;
        int foundLines = 0;

        while (foundLines < 1) {
            read = lineReader.readLine(newLine);
            if (read > 0) {
                pos += read;
                if (newLine.getLength() > 0) {
                    currentRecord.add(newLine.toString());
                    foundLines++;
                }
            }
            else if (read == 0) {
                reachedEof = true;
                break;
            }
        }
    }
    
    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        
        if ((pos >= end) || reachedEof) // Reached the end of split
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
