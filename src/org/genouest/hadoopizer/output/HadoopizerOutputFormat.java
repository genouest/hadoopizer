package org.genouest.hadoopizer.output;

public interface HadoopizerOutputFormat { 
    
    /**
     * @return An ID identifying the type of data that can be written by the current OutputFormat
     */
    public String getId();
}
