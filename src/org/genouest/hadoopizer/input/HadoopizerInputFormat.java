package org.genouest.hadoopizer.input;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

public abstract class HadoopizerInputFormat<K, V> extends FileInputFormat<K, V> {

    private Path headerTempFile;
    
    /**
     * @return An ID identifying the type of data that can be splitted by the current InputFormat
     */
    public abstract String getId();

    /**
     * TODO document
     */
    public Path getHeaderTempFile(Configuration conf) {
        
        if (headerTempFile == null) {
            // No special temp file defined to save header content, use the default one
            String headerFileName = conf.get("hadoopizer.temp.input.header.file");
            if (headerFileName != null &&  !headerFileName.isEmpty())
                headerTempFile = new Path(headerFileName);
        }
        
        return headerTempFile;
    }
    
    /**
     * TODO document
     */
    public void setHeaderTempFile(Path headerTempFile) {
        this.headerTempFile = headerTempFile;
    }
}
