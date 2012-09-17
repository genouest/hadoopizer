package org.genouest.hadoopizer;

import java.util.ServiceLoader;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.genouest.hadoopizer.input.HadoopizerInputFormat;
import org.genouest.hadoopizer.output.HadoopizerOutputFormat;

public class JobOutput {

    private String id;
    private String reducerId;
    private boolean saveAsSequence = false;
    private String localPath = "";

    public JobOutput(String id) {
        
        this.id = id;
        
        if (!id.matches("[A-Za-z0-9]*")) {
            System.err.println("Output id can only contain letters and numbers ('"+id+"')");
            System.exit(1);
        }
    }

    /**
     * Get the path to the local, node-specific output file
     * 
     * @return the localPath
     */
    public String getLocalPath() {
        return localPath;
    }

    /**
     * Set the path to the local, node-specific output file
     * 
     * @param localPath the localPath to set
     */
    public void setLocalPath(String localPath) {
        this.localPath = localPath;
    }

    /**
     * Get the id of this conf output as declared in the xml config file
     * 
     * @return the id
     */
    public String getId() {
        return id;
    }

    /**
     * Get the reducer id
     * 
     * @return the reducerId
     */
    public String getReducerId() {
        return reducerId;
    }

    /**
     * Set the reducer id
     * 
     * @param reducerId the reducerId to set
     */
    public void setReducerId(String reducer) {
        this.reducerId = reducer;
    }

    /**
     * Does the output should be saved in SequenceFile format?
     * 
     * @return true if the output should be saved in SequenceFile format
     */
    // TODO implement loading from sequencefile (with or without join support)
    public boolean isSaveAsSequence() {
        return saveAsSequence;
    }

    /**
     * Set wether the output should be saved in SequenceFile format
     * 
     * @param saveAsSequence true if the output should be saved in SequenceFile format
     */
    public void setSaveAsSequence(boolean saveAsSequence) {
        this.saveAsSequence = saveAsSequence;
    }

    /**
     * Get an FileOutputFormat able to merge the output
     * 
     * @return a FileOutputFormat corresponding to the reducerId defined for this JobOutput
     */
    public HadoopizerOutputFormat getFileOutputFormat() {
        for (HadoopizerOutputFormat outputFormat : ServiceLoader.load(HadoopizerOutputFormat.class)) {
            if (outputFormat.getId().equalsIgnoreCase(getReducerId()) && (FileOutputFormat.class.isAssignableFrom(outputFormat.getClass())))
                return outputFormat;
        }
        
        throw new RuntimeException("Could not find a suitable OutputFormat service for id '" + getReducerId() + "'");
    }

    /**
     * Get an FileInputFormat able to parse the output produced by a map task
     * 
     * @return a FileInputFormat corresponding to the reducerId defined for this JobOutput
     */
    public HadoopizerInputFormat getFileInputFormat() {
        for (HadoopizerInputFormat inputFormat : ServiceLoader.load(HadoopizerInputFormat.class)) {
            if (inputFormat.getId().equalsIgnoreCase(getReducerId()) && (FileInputFormat.class.isAssignableFrom(inputFormat.getClass())))
                return inputFormat;
        }
        
        throw new RuntimeException("Could not find a suitable InputFormat service for id '" + getReducerId() + "'");
    }
}
