package org.genouest.hadoopizer.input;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ObjectWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileRecordReader;
import org.genouest.hadoopizer.JobConfig;
import org.genouest.hadoopizer.JobInputFile;
import org.genouest.hadoopizer.SplitableJobInput;
import org.genouest.hadoopizer.io.TaggedObjectWritable;
import org.genouest.hadoopizer.io.ObjectWritableComparable;

public class TaggedSequenceFileRecordReader extends SequenceFileRecordReader<ObjectWritableComparable, ObjectWritable> {

    protected int inputId = 0; // id corresponding to the input file where we're reading from. This is used to track data origin during map phase

    @Override
    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        super.initialize(split, context);
        
        FileSplit fileSplit = (FileSplit) split;
        trackOrigin(conf, fileSplit.getPath());
    }
    
    /**
     * Keep a track of the file where the Split comes from. You need to call this in the initialize() method. 
     *
     * @param conf the conf
     * @param path the path where we're reading from
     */
    protected void trackOrigin(Configuration conf, Path path) {
        String xmlConfig = conf.get("hadoopizer.job.config");
        JobConfig config = new JobConfig();
        config.load(xmlConfig);
        SplitableJobInput splitable = (SplitableJobInput) config.getSplitableInput();
        int nb = 0;
        for (JobInputFile file : splitable.getFiles()) {
            if (file.getUrl().toString().contentEquals(path.toString())) {
                inputId = nb;
                break;
            }
            
            nb++;
        }
    }

    /**
     * Gets the input id corresponding to the file where the data was read from. 
     *
     * @return the input id
     */
    public int getInputId() {
        return inputId;
    }
    
    @Override
    public ObjectWritable getCurrentValue() {

        TaggedObjectWritable data = new TaggedObjectWritable(getInputId(), super.getCurrentValue());
        return new ObjectWritable(data);
    }
    
}
