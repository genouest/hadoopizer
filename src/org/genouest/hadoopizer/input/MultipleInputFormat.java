package org.genouest.hadoopizer.input;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ObjectWritable;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.genouest.hadoopizer.Hadoopizer;
import org.genouest.hadoopizer.JobConfig;
import org.genouest.hadoopizer.JobInputFile;
import org.genouest.hadoopizer.SplitableJobInput;
import org.genouest.hadoopizer.io.ObjectWritableComparable;

public class MultipleInputFormat extends FileInputFormat<ObjectWritableComparable, ObjectWritable> {

    @Override
    public RecordReader<ObjectWritableComparable, ObjectWritable> createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {

        Configuration conf = context.getConfiguration();
        
        FileSplit fs = (FileSplit) split;
        Path filename = fs.getPath();
        Hadoopizer.logger.info("Looking for input format class for file: " + filename.toString());
        
        // Write data chunk to a temporary input file
        // This input file will be used in the command line launched in the cleanup step
        String xmlConfig = conf.get("hadoopizer.job.config");
        JobConfig config = new JobConfig();
        config.load(xmlConfig);
        
        SplitableJobInput splitable = (SplitableJobInput) config.getSplitableInput();
        int nb = 0;
        for (JobInputFile file : splitable.getFiles()) {
            if (file.getUrl().toString().compareTo(filename.toString()) == 0) {
                HadoopizerInputFormat inf = file.getFileInputFormat();
                
                // There are multiple input: header temp file must have different names
                Path headerFile = new Path(context.getConfiguration().get("hadoopizer.temp.input.header.file") + "_" + splitable.getId() + "_" + nb);
                inf.setHeaderTempFile(headerFile);
                
                Hadoopizer.logger.info("Found an input format class: " + inf.getClass().getCanonicalName());
                return inf.createRecordReader(split, context);
            }
            
            nb++;
        }
        
        System.err.println("Could not find an input format class for: " + filename);
        System.exit(1);
        return null;
    }

    @Override
    protected boolean isSplitable(JobContext context, Path filename) {
        
        CompressionCodec codec = new CompressionCodecFactory(context.getConfiguration()).getCodec(filename);
        
        return codec == null;
    }
}
