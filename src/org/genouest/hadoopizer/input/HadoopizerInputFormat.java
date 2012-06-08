package org.genouest.hadoopizer.input;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ObjectWritable;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.genouest.hadoopizer.io.ObjectWritableComparable;

public abstract class HadoopizerInputFormat extends FileInputFormat<ObjectWritableComparable, ObjectWritable> {

    private Path headerTempFile;
    
    /**
     * Get the ID identifying the type of data that can be splitted by the current InputFormat
     * 
     * @return The id
     */
    public abstract String getId();

    /**
     * Get a file where input file header can be saved
     * 
     * @param conf The conf configuration
     * @return A Path object for the temporary header file
     */
    public Path getHeaderTempFile(Configuration conf) {
        
        if (headerTempFile == null) {
            // No special temp file defined to save header content, use the default one
            String headerFileName = conf.get("hadoopizer.temp.input.header.file"); // FIXME add input id
            if (headerFileName != null &&  !headerFileName.isEmpty())
                headerTempFile = new Path(headerFileName);
        }
        
        return headerTempFile;
    }
    
    /**
     * Set a file where input file header can be saved
     * 
     * @param headerTempFile The path where input header should be saved
     */
    public void setHeaderTempFile(Path headerTempFile) {
        
        this.headerTempFile = headerTempFile;
    }
}
