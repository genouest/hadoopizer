package org.genouest.hadoopizer;

import org.w3c.dom.Document;
import org.w3c.dom.Element;

public abstract class JobInput {

    protected String id;

    public JobInput(String id) {
        this.id = id;
        
        if (!id.matches("[A-Za-z0-9]*")) {
            System.err.println("Input id can only contain letters and numbers ('"+id+"')");
            System.exit(1);
        }
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
    
    /**
     * Generate xml Element representing the current instance.
     *
     * @param doc an xml document where the xml element should be generated
     * @return the element representing the JobInput
     */
    public abstract Element dumpXml(Document doc);

    /**
     * Prepare the command line, replacing variables with local file paths
     *
     * @param cmd the command line as specified in the xml file (with variables)
     * @return the command line with all variables replaced by local paths
     */
    public abstract String prepareCommand(String cmd);

    /**
     * TODO document (and see if it needs refactoring)
     *
     * @param localPath the new local path
     */
    public abstract void setLocalPath(String localPath);
}
