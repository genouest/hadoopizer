package org.genouest.hadoopizer.formats;

public interface HadoopizerOutputFormat { 
    
    /**
     * @return An ID identifying the type of data that can be written by the current OutputFormat
     */
    public String getId();
}
