package org.genouest.hadoopizer.reducer;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.ObjectWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.genouest.hadoopizer.io.ObjectWritableComparable;

/**
 * Reducer that merge multiple values having a same key into an array.
 */
public class JoinReducer extends Reducer<ObjectWritableComparable, ObjectWritable, ObjectWritableComparable, ObjectWritable> {

    @Override
    protected void reduce(ObjectWritableComparable key, Iterable<ObjectWritable> values, Context context) throws IOException, InterruptedException {       

        ArrayList<ObjectWritable> list = new ArrayList<ObjectWritable>();
        for (ObjectWritable value : values) {
            // This is a subtlety in hadoop: it reuses the same 'value' object on each iteration, so we need to copy it instead of writing:
            // list.add(value);
            // See http://cornercases.wordpress.com/2011/08/18/hadoop-object-reuse-pitfall-all-my-reducer-values-are-the-same/ for more infos
            
            list.add(new ObjectWritable(value.get()));
        }
        
        ObjectWritable out = new ObjectWritable(ObjectWritable[].class, (ObjectWritable[]) list.toArray(new ObjectWritable[0]));
        
        context.write(key, out);
    }
}
