package org.genouest.hadoopizer.formats;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.genouest.hadoopizer.Hadoopizer;

public class FastqRecordReader extends RecordReader<LongWritable, Text> {

    private long start;
    private long end;

    private FSDataInputStream fsin;
    private BufferedReader lineReader;
    ArrayList<String> currentRecord;

    private LongWritable recordKey = new LongWritable();
    private Text recordValue = new Text();

    @Override
    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {

        Configuration job = context.getConfiguration();

        FileSplit fileSplit = (FileSplit) split;

        Path path = fileSplit.getPath();
        FileSystem fs = path.getFileSystem(job);
        
        fsin = fs.open(path);
        start = fileSplit.getStart();
        end = fileSplit.getStart() + fileSplit.getLength();
        fsin.seek(start);
        
        lineReader = new BufferedReader(new InputStreamReader(fsin));
        currentRecord = new ArrayList<String>();

        findPotentialFastQRecord();
        if (start != 0) { // Beginning of the whole file
            // Not at the beginning of the file, throw away the first (probably incomplete) line
            shiftFastQRecord();
        }
    }

    /**
     * Reads 4 lines (=possibly a record) from the fastq file
     * 
     * @return a list of 4 strings from the fastq file
     * @throws IOException 
     */
    public void findPotentialFastQRecord() throws IOException {
        currentRecord.clear();
        
        String newLine;
        if ((newLine = lineReader.readLine()) != null)
            currentRecord.add(newLine);
        if ((newLine = lineReader.readLine()) != null)
            currentRecord.add(newLine);
        if ((newLine = lineReader.readLine()) != null)
            currentRecord.add(newLine);
        if ((newLine = lineReader.readLine()) != null)
            currentRecord.add(newLine);
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
        
        String newLine;
        if ((newLine = lineReader.readLine()) != null)
            currentRecord.add(newLine);
    }
    
    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        
        if (fsin.getPos() >= end) // Reached the end of split
            return false;
        
        if (currentRecord.size() != 4)
            return false;
        
        int tries = 0;
        while (tries < 4) { // 4 lines in a record
            if (currentRecord.get(0).startsWith("@") && currentRecord.get(2).startsWith("+") && (currentRecord.get(1).length() == currentRecord.get(3).length())) {
                // This looks like a good FastQ record
                // Construct the string
                String record = "";
                for (String line : currentRecord) {
                    record += line+"\n";
                }
                
                recordKey.set(fsin.getPos());
                recordValue.set(record);
                
                findPotentialFastQRecord(); // Prepare next call
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
    public LongWritable getCurrentKey() throws IOException, InterruptedException {

        return recordKey;
    }

    @Override
    public Text getCurrentValue() throws IOException, InterruptedException {

        return recordValue;
    }

    @Override
    public void close() throws IOException {

        fsin.close();
        lineReader.close();
    }

    @Override
    public float getProgress() throws IOException {

        if (start == end) {
            return (float)0;
        } else {
            return Math.min((float)1.0, (fsin.getPos() - start) / (float)(end - start));
        } 
    }
}
