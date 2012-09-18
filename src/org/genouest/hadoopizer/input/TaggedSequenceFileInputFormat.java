package org.genouest.hadoopizer.input;

import java.io.IOException;

import org.apache.hadoop.io.ObjectWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.genouest.hadoopizer.io.ObjectWritableComparable;

public class TaggedSequenceFileInputFormat extends SequenceFileInputFormat<ObjectWritableComparable, ObjectWritable> {

    @Override
    public RecordReader<ObjectWritableComparable, ObjectWritable> createRecordReader(InputSplit split,
                                                 TaskAttemptContext context
                                                 ) throws IOException {
      return new TaggedSequenceFileRecordReader();
    }
}
