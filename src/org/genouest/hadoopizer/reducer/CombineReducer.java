package org.genouest.hadoopizer.reducer;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.ObjectWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.genouest.hadoopizer.io.ObjectWritableComparable;

// TODO comment
public class CombineReducer extends Reducer<ObjectWritableComparable, ObjectWritable, ObjectWritableComparable, ObjectWritable> {

    @Override
    protected void reduce(ObjectWritableComparable key, Iterable<ObjectWritable> values, Context context) throws IOException, InterruptedException {       

        ArrayList<ObjectWritable> list = new ArrayList<ObjectWritable>();
        for (ObjectWritable value : values) {
            list.add(value);
        } // FIXME problem if the number of values is different from the number of input files
        
        ObjectWritable out = new ObjectWritable(ObjectWritable[].class, (ObjectWritable[]) list.toArray(new ObjectWritable[0]));
        
        context.write(key, out);
    }
}
