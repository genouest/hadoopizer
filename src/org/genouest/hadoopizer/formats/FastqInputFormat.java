package org.genouest.hadoopizer.formats;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;

public class FastqInputFormat extends FileInputFormat<Text, Text> implements HadoopizerInputFormat {

    @Override
    public RecordReader<Text, Text> createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {

        // TODO add support for compressed input
        context.setStatus("Received a fastq split of length: " + split.getLength());
        return new FastqRecordReader();
    }

    @Override
    public String getId() {
        
        return "fastq";
    }
}
