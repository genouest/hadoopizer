package org.genouest.hadoopizer;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;

public class SplitableJobInput extends JobInput {
    
    private ArrayList<JobInputFile> files = new ArrayList<JobInputFile>();

    public SplitableJobInput(String id) {
        
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
        for (int i = 0; i < urls.getLength(); i++) {
            Element urlEl = (Element) urls.item(i);
            String url = urls.item(i).getTextContent();
            
            if (url.startsWith("/"))
                url = "file:" + url;
            
            if (!urlEl.hasAttribute("splitter") || urlEl.getAttribute("splitter").contentEquals("")) {
                System.err.println("You must specify a splitter for url: "+url);
                System.exit(1);
            }
            
            JobInputFile file = new JobInputFile();
            try {
                file.setUrl(new URI(url));
                file.setAutoComplete(false); // No autocomplete for splitable input
                file.setSplitterId(urlEl.getAttribute("splitter"));
                files.add(file);
            } catch (URISyntaxException e) {
                System.err.println("Wrong URI format in config file: "+url);
                e.printStackTrace();
                System.exit(1);
            }

            if (urlEl.hasAttribute("sequence") && urlEl.getAttribute("sequence").equalsIgnoreCase("true")) {
                // The input needs to be loaded from hadoop SequenceFile
                file.setLoadAsSequence(true);
            }
            
            Hadoopizer.logger.info("Using splitter '"+file.getSplitterId()+"' for input '"+getId()+"' ("+file.getUrl()+")");
        }
    }

    @Override
    public Element dumpXml(Document doc) {

        Element inputElement = doc.createElement("input");
        inputElement.setAttribute("id", getId());
        inputElement.setAttribute("split", "true");


        for (JobInputFile file : files) {
            Element urlElement = doc.createElement("url");
            inputElement.appendChild(urlElement);
            urlElement.appendChild(doc.createTextNode(file.getUrl().toString()));
            if (file.isAutoComplete()) {
                urlElement.setAttribute("autocomplete", "true");
            }
            if (file.isLoadAsSequence()) {
                urlElement.setAttribute("sequence", "true");
            }
            
            urlElement.setAttribute("splitter", file.getSplitterId());
        }
        
        return inputElement;
    }

    @Override
    public String prepareCommand(String cmd) {

        int nb = 1;
        
        for (JobInputFile file : files) {
            if (file.getLocalPath().isEmpty())
                throw new RuntimeException("Unable to generate command line: the splitable input local path is empty for url '" + file.getUrl() + "'.");
        
            String multipleSuffix = "";
            if (needJoin())
                multipleSuffix = "\\#" + nb;
            cmd = cmd.replaceAll("\\$\\{" + getId() + multipleSuffix + "\\}", file.getLocalPath());
                        
            nb++;
        }
        
        return cmd;
    }
    
    /**
     * Gets the input files containing the data to split.
     *
     * @return the files
     */
    public ArrayList<JobInputFile> getFiles() {
        
        return files;
    }
    
    /**
     * Check if the data represented by current instance needs to be joined before being usable for command line
     *
     * @return true, if the data need to be joined
     */
    public boolean needJoin() {
        
        return files.size() > 1;
    }
}
