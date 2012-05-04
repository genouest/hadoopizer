package org.genouest.hadoopizer;

import java.net.URI;

public class JobOutput {

    private String id;
    private URI url;
    private String reducer; // TODO replace with reducer instance
    private String localPath;

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

}
