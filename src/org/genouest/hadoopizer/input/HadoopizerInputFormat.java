package org.genouest.hadoopizer.input;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ObjectWritable;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.genouest.hadoopizer.JobConfig;
import org.genouest.hadoopizer.JobInputFile;
import org.genouest.hadoopizer.SplitableJobInput;
import org.genouest.hadoopizer.io.ObjectWritableComparable;

public abstract class HadoopizerInputFormat extends FileInputFormat<ObjectWritableComparable, ObjectWritable> {

    private Path headerTempFile;
    
    /**
     * Get the ID identifying the type of data that can be splited by the current InputFormat
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
    // TODO update doc
    public Path getHeaderTempFile(Configuration conf, Path file) {
        
        if (headerTempFile == null) {

            // No special temp file defined to save header content, use the default one
            
            // Write data chunk to a temporary input file
            // This input file will be used in the command line launched in the cleanup step
            String xmlConfig = conf.get("hadoopizer.job.config");
            JobConfig config = new JobConfig();
            config.load(xmlConfig);
            
            SplitableJobInput splitable = (SplitableJobInput) config.getSplitableInput();
            int nb = 0;
            for (JobInputFile inFile : splitable.getFiles()) {
                if (inFile.getUrl().toString().compareTo(file.toString()) == 0) {
                    // There are multiple input: header temp file must have different names
                    String headerFile = conf.get("hadoopizer.temp.input.header.file") + "_" + splitable.getId() + "_" + nb;
                    if (headerFile != null &&  !headerFile.isEmpty())
                        headerTempFile = new Path(headerFile);
                }
                
                nb++;
            }
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
