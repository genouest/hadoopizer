package org.genouest.hadoopizer;

import java.net.URI;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.ServiceLoader;

import org.apache.hadoop.io.compress.BZip2Codec;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.genouest.hadoopizer.output.HadoopizerOutputFormat;
import org.genouest.hadoopizer.parsers.OutputParser;

public class JobOutput {

    private String id;
    private URI url;
    private String reducer;
    private boolean sequenceOutput = false;
    private String localPath = "";
    private String compressor;
    
    private static final HashMap<String, Class<? extends CompressionCodec>> allowedCodecs = new HashMap<String, Class<? extends CompressionCodec>>();
    static {
        allowedCodecs.put("gzip", GzipCodec.class);
        allowedCodecs.put("bzip2", BZip2Codec.class);
    }

    public JobOutput(String id) {
        this.id = id;
    }

    /**
     * @return the reducer
     */
    public String getReducer() {
        return reducer;
    }

    /**
     * @param reducer the reducer to set
     */
    public void setReducer(String reducer) {
        this.reducer = reducer;
    }

    /**
     * @return the id
     */
    public String getId() {
        return id;
    }

    /**
     * @return the url
     */
    public URI getUrl() {
        return url;
    }

    /**
     * @param url the url to set
     */
    public void setUrl(URI url) {
        this.url = url;
    }

    /**
     * @return the localPath
     */
    public String getLocalPath() {
        return localPath;
    }

    /**
     * @param localPath the localPath to set
     */
    public void setLocalPath(String localPath) {
        this.localPath = localPath;
    }

    /**
     * @return the sequenceOutput
     */
    public boolean isSequenceOutput() {
        return sequenceOutput;
    }

    /**
     * @param sequenceOutput the sequenceOutput to set
     */
    public void setSequenceOutput(boolean sequenceOutput) {
        this.sequenceOutput = sequenceOutput;
    }

    /**
     * Get an FileOutputFormat able to merge the output
     * 
     * @return an FileOutputFormat corresponding to the reducer defined for this JobOutput
     */
    public FileOutputFormat<?, ?> getFileOutputFormat() {
        HadoopizerOutputFormat outputFormat = null;

        ServiceLoader<HadoopizerOutputFormat> serviceLoader = ServiceLoader.load(HadoopizerOutputFormat.class);
        Iterator<HadoopizerOutputFormat> iterator = serviceLoader.iterator();
        while (iterator.hasNext()) {
            outputFormat = iterator.next();
            if (outputFormat.getId().equalsIgnoreCase(getReducer()) && (FileOutputFormat.class.isAssignableFrom(outputFormat.getClass())))
                return (FileOutputFormat<?, ?>) outputFormat;
        }
        
        throw new RuntimeException("Could not find a suitable OutputFormat service for id '" + getReducer() + "'");
    }


    /**
     * Get an OutputParser able to split the command output in key/values
     * 
     * @return an OutputParser corresponding to the reducer defined for this JobOutput
     */
    public OutputParser getOutputParser() {
        OutputParser outputParser = null;

        ServiceLoader<OutputParser> serviceLoader = ServiceLoader.load(OutputParser.class);
        Iterator<OutputParser> iterator = serviceLoader.iterator();
        while (iterator.hasNext()) {
            outputParser = iterator.next();
            if (outputParser.getId().equalsIgnoreCase(getReducer()))
                return outputParser;
        }
        
        throw new RuntimeException("Could not find a suitable OutputParser service for id '" + getReducer() + "'");
    }

    /**
     * @return The name of the compressor to use
     */
    public String getCompressorName() {
        return compressor;
    }
    
    /**
     * @return the compressor
     */
    public Class<? extends CompressionCodec> getCompressor() {
        if (allowedCodecs.containsKey(compressor))
            return allowedCodecs.get(compressor);
        
        return null;
    }

    /**
     * @param compressor the compressor to set
     */
    public void setCompressor(String compressor) {
        this.compressor = compressor;
    }

    /**
     * @return true if a compressor is set
     */
    public boolean hasCompressor() {
        return allowedCodecs.containsKey(compressor);
    }
    
    /**
     * @return true if given compressor name is supported
     */
    public static boolean isCompressorSupported(String compressor) {
        return allowedCodecs.containsKey(compressor);
    }

    public static String getSupportedCompressor() {
        String sup = "";
        
        for (Entry<String, Class<? extends CompressionCodec>> e : allowedCodecs.entrySet()) {
            sup += e.getKey() + " ";
        }
        
        return sup;
    }
}
