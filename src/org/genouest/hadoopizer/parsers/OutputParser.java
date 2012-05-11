package org.genouest.hadoopizer.parsers;

import java.io.File;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public interface OutputParser {

    /**
     * @return An ID identifying the type of data that can be splitted by the current InputFormat
     */
    public String getId();
    
    /**
     * Parse an output file in a specific format and add each key/value to a Mapper context
     * 
     * @param file
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    public void parse(File file, Mapper<?, ?, Text, Text>.Context context) throws IOException, InterruptedException; // FIXME  will we need another type some day?
}
