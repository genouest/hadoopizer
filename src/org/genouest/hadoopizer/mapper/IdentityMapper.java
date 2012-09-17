package org.genouest.hadoopizer.mapper;

import java.io.IOException;

import org.apache.hadoop.io.ObjectWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.genouest.hadoopizer.io.ObjectWritableComparable;


/**
 * The Class IdentityMapper: don't change anything to key-value received.
 */

public class IdentityMapper extends Mapper<ObjectWritableComparable, ObjectWritable, ObjectWritableComparable, ObjectWritable> {


    @Override
    protected void map(ObjectWritableComparable key, ObjectWritable value, Context context) throws IOException, InterruptedException {
        context.write(key, value);
    }
}
