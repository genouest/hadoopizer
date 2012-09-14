package org.genouest.hadoopizer.input;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ObjectWritable;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.genouest.hadoopizer.io.ObjectWritableComparable;

public class PairedFastqInputFormat extends HadoopizerInputFormat {

    @Override
    public RecordReader<ObjectWritableComparable, ObjectWritable> createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {

        Configuration conf = context.getConfiguration();
        
        FileSplit fs = (FileSplit) split;
        Path filename = fs.getPath();
        
        return new PairedFastqRecordReader(getHeaderTempFile(conf, filename), conf);
    }

    @Override
    protected boolean isSplitable(JobContext context, Path filename) {
        
        CompressionCodec codec = new CompressionCodecFactory(context.getConfiguration()).getCodec(filename);
        
        return codec == null;
    }

    @Override
    public String getId() {
        
        return "paired_fastq";
    }
}
