package org.genouest.hadoopizer;

import java.io.File;
import java.net.URI;
import java.util.HashSet;
import java.util.Iterator;
import java.util.ServiceLoader;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.genouest.hadoopizer.formats.HadoopizerInputFormat;

public class JobInput {

    private String id;
    private URI url;
    private String splitter;
    private String localPath;
    private boolean autoComplete = false;

    public JobInput(String id) {
        this.id = id;
    }

    /**
     * @return true if there is a splitter associated to this input
     */
    public boolean hasSplitter() {
        return (splitter != null && !splitter.isEmpty());
    }

    /**
     * @return the splitter
     */
    public String getSplitter() {
        return splitter;
    }

    /**
     * @param splitter the splitter to set
     */
    public void setSplitter(String splitter) {
        this.splitter = splitter;
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
     * Return a list of all urls matching this input.
     * If autocomplete is false, the list contains only 1 element (same as getUrl()).
     * Otherwise, it will try to return all the files beginning with what is returned by getUrl(). 
     * @return the list of input url
     */
    public HashSet<URI> getAllUrls() {

        HashSet<URI> urls = new HashSet<URI>();

        Path basePath = new Path(url);
        String filePrefix = basePath.getName();
        File dir = new File(basePath.getParent().toUri());

        File[] listOfFiles = dir.listFiles();
        for (int i = 0; i < listOfFiles.length; i++) {
            if (listOfFiles[i].isFile() && listOfFiles[i].getName().startsWith(filePrefix))
                urls.add(listOfFiles[i].toURI());
        }

        return urls;
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
     * @return the autoComplete
     */
    public boolean isAutoComplete() {
        return autoComplete;
    }

    /**
     * @param autoComplete the autoComplete to set
     */
    public void setAutoComplete(boolean autoComplete) {
        this.autoComplete = autoComplete;
    }

    /**
     * Get an FileInputFormat able to split the splittable input
     * 
     * @return an FileInputFormat corresponding to the splitter defined for this JobInput
     */
    public FileInputFormat<?, ?> getFileInputFormat() {
        HadoopizerInputFormat inputFormat = null;

        ServiceLoader<HadoopizerInputFormat> serviceLoader = ServiceLoader.load(HadoopizerInputFormat.class);
        Iterator<HadoopizerInputFormat> iterator = serviceLoader.iterator();
        while (iterator.hasNext()) {
            inputFormat = iterator.next();
            if (inputFormat.getId().equalsIgnoreCase(getSplitter()) && (FileInputFormat.class.isAssignableFrom(inputFormat.getClass())))
                return (FileInputFormat<?, ?>) inputFormat;
        }
        
        throw new RuntimeException("Could not find a suitable InputFormat service for id '" + getSplitter() + "'");
    }
}
