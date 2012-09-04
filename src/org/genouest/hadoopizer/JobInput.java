package org.genouest.hadoopizer;

import org.w3c.dom.Document;
import org.w3c.dom.Element;

public abstract class JobInput {

    protected String id;

    public JobInput(String id) {
        this.id = id;
    }

    /**
     * Get the id of this conf input as declared in the xml config file
     * 
     * @return the id
     */
    public String getId() {
        return id;
    }

    /**
     * Load a JobInput from xml content
     * 
     * @param input the xml Element to load
     */
    public abstract void loadXml(Element input);
    
    // TODO document
    public abstract Element dumpXml(Document doc);

    //TODO document
    public abstract String prepareCommand(String cmd);

    /**
     * TODO document
     */
    public abstract void setLocalPath(String localPath);
}
