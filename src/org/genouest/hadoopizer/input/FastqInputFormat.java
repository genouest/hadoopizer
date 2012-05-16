package org.genouest.hadoopizer.input;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;

public class FastqInputFormat extends FileInputFormat<Text, Text> implements HadoopizerInputFormat {

    @Override
    public RecordReader<Text, Text> createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {

        context.setStatus("Received a fastq split of length: " + split.getLength());
        return new FastqRecordReader();
    }

    @Override
    protected boolean isSplitable(JobContext context, Path filename) {
        CompressionCodec codec = new CompressionCodecFactory(context.getConfiguration()).getCodec(filename);
        return codec == null;
    }

    @Override
    public String getId() {
        
        return "fastq";
    }
}
