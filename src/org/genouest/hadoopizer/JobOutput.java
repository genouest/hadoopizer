package org.genouest.hadoopizer;

import java.net.URI;
import java.util.HashMap;
import java.util.Map.Entry;
import java.util.ServiceLoader;

import org.apache.hadoop.io.compress.BZip2Codec;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.genouest.hadoopizer.input.HadoopizerInputFormat;
import org.genouest.hadoopizer.output.HadoopizerOutputFormat;

public class JobOutput {

    private String id;
    private URI url;
    private String reducerId;
    private boolean sequenceOutput = false;
    private String localPath = "";
    private String compressor;
    
    /**
     * List of allowed compression codecs
     */
    private static final HashMap<String, Class<? extends CompressionCodec>> allowedCodecs = new HashMap<String, Class<? extends CompressionCodec>>();
    static {
        allowedCodecs.put("gzip", GzipCodec.class);
        allowedCodecs.put("bzip2", BZip2Codec.class);
    }

    public JobOutput(String id) {
        this.id = id;
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
     * Get the url where the output data will be saved, as declared in the config
     * 
     * @return the url
     */
    public URI getUrl() {
        return url;
    }

    /**
     * Set the url where the output data will be saved, as declared in the config
     * 
     * @param url the url to set
     */
    public void setUrl(URI url) {
        this.url = url;
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
     * Does the output should be saved in SequenceFile format?
     * 
     * @return true if the output should be saved in SequenceFile format
     */
    public boolean isSequenceOutput() {
        // TODO implement saving as sequencefile
        return sequenceOutput;
    }

    /**
     * Set wether the output should be saved in SequenceFile format
     * 
     * @param sequenceOutput true if the output should be saved in SequenceFile format
     */
    public void setSequenceOutput(boolean sequenceOutput) {
        this.sequenceOutput = sequenceOutput;
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

    /**
     * Get the name of the compressor to use
     * 
     * @return The name of the compressor to use
     */
    public String getCompressorName() {
        return compressor;
    }
    
    /**
     * Get the compressor class
     * 
     * @return the compressor class
     */
    public Class<? extends CompressionCodec> getCompressor() {
        if (allowedCodecs.containsKey(compressor))
            return allowedCodecs.get(compressor);
        
        return null;
    }

    /**
     * Set the name of the compressor to use
     * 
     * @param compressor the compressor to set
     */
    public void setCompressor(String compressor) {
        this.compressor = compressor;
    }

    /**
     * Is there a compressor for this output
     * 
     * @return true if a compressor is set
     */
    public boolean hasCompressor() {
        return allowedCodecs.containsKey(compressor);
    }
    
    /**
     * Is the given compressor name is supported
     * 
     * @return true if given compressor name is supported
     */
    public static boolean isCompressorSupported(String compressor) {
        return allowedCodecs.containsKey(compressor);
    }

    /**
     * Get a list of supported compressors
     * 
     * @return a comma separated list of supported compressors
     */
    public static String getSupportedCompressor() {
        String sup = "";
        
        for (Entry<String, Class<? extends CompressionCodec>> e : allowedCodecs.entrySet()) {
            sup += e.getKey() + " ";
        }
        
        return sup;
    }
}
