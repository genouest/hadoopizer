package org.genouest.hadoopizer.input;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

public abstract class HadoopizerInputFormat<K, V> extends FileInputFormat<K, V> { 
    
    /**
     * @return An ID identifying the type of data that can be splitted by the current InputFormat
     */
    public abstract String getId();
    
}
