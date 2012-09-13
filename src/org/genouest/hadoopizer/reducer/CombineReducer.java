package org.genouest.hadoopizer.reducer;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.ObjectWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.genouest.hadoopizer.Hadoopizer;
import org.genouest.hadoopizer.io.ObjectWritableComparable;

// TODO comment
public class CombineReducer extends Reducer<ObjectWritableComparable, ObjectWritable, ObjectWritableComparable, ObjectWritable> {

    @Override
    protected void reduce(ObjectWritableComparable key, Iterable<ObjectWritable> values, Context context) throws IOException, InterruptedException {       

        ArrayList<ObjectWritable> list = new ArrayList<ObjectWritable>();
        Hadoopizer.logger.info("for key: " + key.toString());
        for (ObjectWritable value : values) {
            list.add(value);
            Hadoopizer.logger.info("received value: " + value.toString());
        }
        
        ObjectWritable out = new ObjectWritable(ObjectWritable[].class, (ObjectWritable[]) list.toArray(new ObjectWritable[0]));
        
        context.write(key, out);
    }
}
