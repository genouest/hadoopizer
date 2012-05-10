package org.genouest.hadoopizer.formats;

public interface HadoopizerInputFormat { 
    
    /**
     * @return An ID identifying the type of data that can be splitted by the current InputFormat
     */
    public String getId();
}
