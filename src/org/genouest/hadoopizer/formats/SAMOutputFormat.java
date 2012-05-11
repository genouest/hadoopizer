package org.genouest.hadoopizer.formats;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class SAMOutputFormat extends FileOutputFormat<Text, Text> implements HadoopizerOutputFormat {

    // FIXME add support for compression (see TextOutputFormat implementation)
    
    @Override
    public RecordWriter<Text, Text> getRecordWriter(TaskAttemptContext context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        Path path = getDefaultWorkFile(context, ".sam");
        FileSystem fs = path.getFileSystem(conf);
        
        FSDataOutputStream out = fs.create(path, false);

        return new SAMRecordWriter(out);
    }

    @Override
    public String getId() {
        
        return "sam";
    }

}
