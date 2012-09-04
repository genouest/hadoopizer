package org.genouest.hadoopizer;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashSet;

import org.apache.hadoop.conf.Configuration;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;

public class StaticJobInput extends JobInput {

    private JobInputFile file;
    
    public StaticJobInput(String id) {
        
        super(id);
    }

    /**
     * Load a JobInput from xml content
     * 
     * @param input the xml Element to load
     */
    @Override
    public void loadXml(Element input) {

        NodeList urls = input.getElementsByTagName("url");
        
        if (urls.getLength() != 1) {
            System.err.println("Static input '" + getId() + "' need exactly one url element");
            System.exit(1);
        }
        
        String url = urls.item(0).getTextContent();
        
        if (url.startsWith("/"))
            url = "file:" + url;
        
        file = new JobInputFile();
        try {
            file.setUrl(new URI(url));
            Element urlEl = (Element) urls.item(0);
            file.setAutoComplete(urlEl.hasAttribute("autocomplete") && urlEl.getAttribute("autocomplete").equalsIgnoreCase("true"));
        } catch (URISyntaxException e) {
            System.err.println("Wrong URI format in config file: "+url);
            e.printStackTrace();
            System.exit(1);
        }
        
        Hadoopizer.logger.info("No splitting for input '"+getId()+"' ("+file.getUrl()+")");
    }

    @Override
    public Element dumpXml(Document doc) {

        Element inputElement = doc.createElement("input");
        inputElement.setAttribute("id", getId());

        Element urlElement = doc.createElement("url");
        inputElement.appendChild(urlElement);
        urlElement.appendChild(doc.createTextNode(file.getUrl().toString()));
        if (file.isAutoComplete()) {
            urlElement.setAttribute("autocomplete", "true");
        }
        
        return inputElement;
    }

    @Override
    public String prepareCommand(String cmd) {
        
        if (file.getLocalPath().isEmpty())
            throw new RuntimeException("Unable to generate command line: the '" + getId() + "' input local path is empty.");
    
        cmd = cmd.replaceAll("\\$\\{" + getId() + "\\}", file.getLocalPath());
        
        return cmd;
    }

    @Override
    public void setLocalPath(String localPath) {
        
        file.setLocalPath(localPath);
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
        
        return file.getAllUrls(jobConf);
    }
}
