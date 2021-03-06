package org.genouest.hadoopizer;

import java.io.IOException;
import java.net.URI;
import java.util.HashSet;
import java.util.ServiceLoader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.genouest.hadoopizer.input.HadoopizerInputFormat;
import org.genouest.hadoopizer.output.HadoopizerOutputFormat;

public class JobInputFile {

    private URI url;
    private boolean autoComplete = false;
    private boolean loadAsSequence = false;
    private String splitterId;
    private String localPath;
    
    /**
     * Get the url where the input data can be found, as declared in the config
     * 
     * @return the url
     */
    public URI getUrl() {
        return url;
    }

    /**
     * Set the url where the input data can be found, as declared in the config
     * 
     * @param url the url to set
     */
    public void setUrl(URI url) {
        this.url = url;
    }

    /**
     * Return a list of all urls matching this input.
     * If autocomplete is false, the list contains only 1 element (same as getUrl()).
     * Otherwise, it will try to return all the files beginning with what is returned by getUrl().
     *  
     * @param jobConf A Configuration object
     * @return the list of input url
     */
    public HashSet<URI> getAllUrls(Configuration jobConf) {

        HashSet<URI> urls = new HashSet<URI>();

        if (!isAutoComplete()) {
            urls.add(url);
        }
        else {
            Path basePath = new Path(url);
            String filePrefix = basePath.getName();
    
            try {
                FileSystem fs = basePath.getFileSystem(jobConf);
                
                if (!fs.exists(basePath.getParent())) {
                    throw new IOException("Input directory not found: " + url);
                }
                    
                FileStatus[] stats = fs.listStatus(basePath.getParent());
                 
                for (int i = 0; i < stats.length; i++) {
                    Path path = stats[i].getPath();
                    if (fs.isFile(path) && path.getName().startsWith(filePrefix))
                        urls.add(path.toUri());
                }
            } catch (IOException e) {
                System.err.println("Unable to autocomplete input file");
                e.printStackTrace();
                System.exit(1);
            }
        }

        return urls;
    }

    /**
     * Does this JobInput represent a single input file, or a set of input files with the same prefix?
     * 
     * @return true if it is a set of file, false if it's a single one
     */
    public boolean isAutoComplete() {
        return autoComplete;
    }

    /**
     * Set the wether this JobInput represent a single input file, or a set of input files with the same prefix
     * 
     * @param autoComplete true if the file is a set of files, false otherwise
     */
    public void setAutoComplete(boolean autoComplete) {
        this.autoComplete = autoComplete;
    }

    /**
     * Get the path to the local, node-specific file containing the input data
     * 
     * @return the localPath
     */
    public String getLocalPath() {
        return localPath;
    }

    /**
     * Set the path to the local, node-specific file containing the input data
     * 
     * @param localPath the localPath to set
     */
    public void setLocalPath(String localPath) {
        if (isAutoComplete()) {
            // In the config, the url looks like: /the/local/path/filenameprefix
            // We receive something like: /some/path/static_input__someid__filenameprefix.txt
            // We want to keep: /some/path/static_input__someid__filenameprefix
            Path local = new Path(localPath);
            Path source = new Path(url);
            int posMiddle = local.getName().lastIndexOf(source.getName());
            String prefix = local.getName().substring(0, posMiddle);
            this.localPath = local.getParent().toString() + Path.SEPARATOR + prefix + source.getName();
        }
        else
            this.localPath = localPath;
    }

    /**
     * @return true if there is a splitter associated to this input
     */
    public boolean hasSplitter() {
        return (splitterId != null && !splitterId.isEmpty());
    }

    /**
     * Get the splitter id
     * 
     * @return the splitter id
     */
    public String getSplitterId() {
        return splitterId;
    }

    /**
     * Set the splitter id
     * 
     * @param splitter the splitter id to set
     */
    public void setSplitterId(String splitter) {
        this.splitterId = splitter;
    }

    /**
     * Get a FileInputFormat instance able to split the splitable input
     * 
     * @return a FileInputFormat corresponding to the splitter id defined for this JobInput
     */
    public HadoopizerInputFormat getFileInputFormat() {
        
        for (HadoopizerInputFormat inputFormat : ServiceLoader.load(HadoopizerInputFormat.class)) {
            if (inputFormat.getId().equalsIgnoreCase(getSplitterId()) && (FileInputFormat.class.isAssignableFrom(inputFormat.getClass())))
                return inputFormat;
        }
        
        throw new RuntimeException("Could not find a suitable InputFormat service for id '" + getSplitterId() + "'");
    }

    /**
     * Get a FileOutputFormat instance able to write the splitable input into a temporary file
     * 
     * @return a FileOutputFormat corresponding to the splitter id defined for this JobInput
     */
    public HadoopizerOutputFormat getFileOutputFormat() {
        
        for (HadoopizerOutputFormat outputFormat : ServiceLoader.load(HadoopizerOutputFormat.class)) {
            if (outputFormat.getId().equalsIgnoreCase(getSplitterId()) && (FileOutputFormat.class.isAssignableFrom(outputFormat.getClass())))
                return outputFormat;
        }
        
        throw new RuntimeException("Could not find a suitable OutputFormat service for id '" + getSplitterId() + "'");
    }

    /**
     * Does the input should be loaded from SequenceFile format?
     * Doesn't make sense for static files
     * 
     * @return true if the input should be loaded from SequenceFile format
     */
    public boolean isLoadAsSequence() {
        return loadAsSequence;
    }

    /**
     * Set whether the input should be loaded from SequenceFile format
     * Doesn't make sense for static files
     * 
     * @param loadAsSequence true if the input should be loaded from SequenceFile format
     */
    public void setLoadAsSequence(boolean loadAsSequence) {
        this.loadAsSequence = loadAsSequence;
    }
}
