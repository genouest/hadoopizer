package org.genouest.hadoopizer.reducer;

import java.io.IOException;

import org.apache.hadoop.io.ObjectWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.genouest.hadoopizer.io.ObjectWritableComparable;

public class ShellReducer extends Reducer<ObjectWritableComparable, ObjectWritable, ObjectWritableComparable, ObjectWritable> {
    
    private MultipleOutputs<ObjectWritableComparable, ObjectWritable> mos;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        
        mos = new MultipleOutputs<ObjectWritableComparable, ObjectWritable>(context);
    }

    @Override
    protected void reduce(ObjectWritableComparable key, Iterable<ObjectWritable> values, Context context) throws IOException, InterruptedException {
        
        for (ObjectWritable valuein : values) {
            mos.write(key.getKeyType(), key, valuein);
        }
    }
    
    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        
        mos.close();
    }
}
