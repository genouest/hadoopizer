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

public class ChainValuesOutputFormat extends FileOutputFormat<Text, Text> {

	@Override
	public RecordWriter<Text, Text> getRecordWriter(TaskAttemptContext context) throws IOException, InterruptedException {
		Configuration conf = context.getConfiguration();
		Path path = getOutputPath(context);
		FileSystem fs = path.getFileSystem(conf);
		FSDataOutputStream out = fs.create(new Path(path, context.getJobName() + "_results.out")); // FIXME better filename
		
		return new ChainValuesRecordWriter(out);
	}

}
