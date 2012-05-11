package org.genouest.hadoopizer;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringWriter;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerConfigurationException;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpression;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

public class JobConfig {

    private String command;
    private JobInput splittableInput;
    private HashSet<JobInput> staticInputs;
    private JobOutput jobOutput;
    private HashMap<String, String> hadoopConfig;

    public JobConfig() {

        staticInputs = new HashSet<JobInput>();
        hadoopConfig = new HashMap<String, String>();
    }
    
    /**
     * @param configFile
     * @throws FileNotFoundException
     */
    public void load(File configFile) throws FileNotFoundException {

        FileInputStream fis = new FileInputStream(configFile);
        load(fis);
    }

    /**
     * @param configContent
     */
    public void load(String configContent) {

        InputStream is = new ByteArrayInputStream(configContent.getBytes());
        load(is);
    }

    /**
     * @param configContent
     */
    public void load(InputStream configContent) {

        // TODO validate using xml schema

        // Open xml config file
        DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
        XPathFactory xPathfactory = XPathFactory.newInstance();
        XPath xpath = xPathfactory.newXPath();
        Document doc = null;

        try {
            DocumentBuilder builder = factory.newDocumentBuilder();
            doc = builder.parse(configContent);
        } catch (ParserConfigurationException e) {
            e.printStackTrace();
            System.exit(1);
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(1);
        } catch (SAXException e) {
            e.printStackTrace();
            System.exit(1);
        }

        // Get the command
        try {
            XPathExpression expr = xpath.compile("/job/command");
            command = (String) expr.evaluate(doc, XPathConstants.STRING);
        } catch (XPathExpressionException e) {
            e.printStackTrace();
            System.exit(1);
        }

        command = command.trim();

        // Get the inputs
        NodeList inputs = null;
        try {
            XPathExpression expr = xpath.compile("/job/input");
            inputs = (NodeList) expr.evaluate(doc, XPathConstants.NODESET);
        } catch (XPathExpressionException e) {
            e.printStackTrace();
            System.exit(1);
        }

        for(int i = 0; i < inputs.getLength(); i++) {
            Element input = (Element) inputs.item(i);

            JobInput jobInput = new JobInput(input.getAttribute("id"));
            if (!input.getElementsByTagName("splitter").item(0).getTextContent().equalsIgnoreCase("none")) {
                jobInput.setSplitter(input.getElementsByTagName("splitter").item(0).getTextContent());
            }

            String url = input.getElementsByTagName("url").item(0).getTextContent();
            if (url.startsWith("/")) // FIXME handle other schemes
                url = "file:" + url;
            try {
                jobInput.setUrl(new URI(url));
            } catch (URISyntaxException e) {
                System.err.println("Wrong URI format in config file: "+input.getElementsByTagName("url").item(0).getTextContent());
                e.printStackTrace();
                System.exit(1);
            }

            Element urlEl = (Element) input.getElementsByTagName("url").item(0);

            if (urlEl.hasAttribute("autocomplete") && urlEl.getAttribute("autocomplete").equalsIgnoreCase("true")) {
                jobInput.setAutoComplete(true);
            }

            if (jobInput.hasSplitter()) {
                Hadoopizer.logger.info("Using splitter '"+jobInput.getSplitter()+"' for input '"+jobInput.getId()+"' ("+jobInput.getUrl()+")");
                splittableInput = jobInput;
            }
            else {
                Hadoopizer.logger.info("No splitting for input '"+jobInput.getId()+"' ("+jobInput.getUrl()+")");			
                staticInputs.add(jobInput);
            }
        }

        // Get the output
        NodeList outputs = null;
        try {
            XPathExpression expr = xpath.compile("/job/output");
            outputs = (NodeList) expr.evaluate(doc, XPathConstants.NODESET);
        } catch (XPathExpressionException e) {
            e.printStackTrace();
            System.exit(1);
        }

        if (outputs.getLength() != 1) {
            System.err.println("The config file should contain exactly one output element");
            System.exit(1);
        }

        Element output = (Element) outputs.item(0);

        jobOutput = new JobOutput(output.getAttribute("id"));
        Element reducer = (Element) output.getElementsByTagName("reducer").item(0);
        jobOutput.setReducer(reducer.getTextContent());
        if (reducer.hasAttribute("format") && reducer.getAttribute("format").equalsIgnoreCase("sequence")) {
            // The output needs to be stored as hadoop SequenceFile for reuse in a future hadoop job
            jobOutput.setSequenceOutput(true);
        }

        String url = output.getElementsByTagName("url").item(0).getTextContent();
        if (url.startsWith("/"))
            url = "file:" + url;

        try {
            jobOutput.setUrl(new URI(url));
        } catch (URISyntaxException e) {
            System.err.println("Wrong URI format in config file: "+output.getElementsByTagName("url").item(0).getTextContent());
            e.printStackTrace();
            System.exit(1);
        }

        Hadoopizer.logger.info("Using reducer '"+jobOutput.getReducer()+"' for output '"+jobOutput.getId()+"' ("+jobOutput.getUrl()+")");
        
        // Load hadoop specific configuration
        NodeList hadoops = null;
        try {
            XPathExpression expr = xpath.compile("/job/hadoop/config");
            hadoops = (NodeList) expr.evaluate(doc, XPathConstants.NODESET);
        } catch (XPathExpressionException e) {
            e.printStackTrace();
            System.exit(1);
        }
        
        for(int i = 0; i < hadoops.getLength(); i++) {
            Element hConf = (Element) hadoops.item(i);
            if (hConf.hasAttribute("key") && !hConf.getAttribute("key").isEmpty() && !hConf.getTextContent().isEmpty()) {
                hadoopConfig.put(hConf.getAttribute("key"), hConf.getTextContent());
            }
        }
    }

    /**
     * @return
     * @throws ParserConfigurationException
     */
    public String dumpXml() throws ParserConfigurationException {
        DocumentBuilderFactory docFactory = DocumentBuilderFactory.newInstance();
        DocumentBuilder docBuilder = docFactory.newDocumentBuilder();

        Document doc = docBuilder.newDocument();

        Element rootElement = doc.createElement("job");
        doc.appendChild(rootElement);

        Element commandElement = doc.createElement("command");
        rootElement.appendChild(commandElement);
        commandElement.appendChild(doc.createTextNode(getRawCommand()));

        // Static inputs
        for (JobInput input : staticInputs) {
            Element inputElement = doc.createElement("input");
            rootElement.appendChild(inputElement);
            inputElement.setAttribute("id", input.getId());

            Element splitter = doc.createElement("splitter");
            if (input.hasSplitter()) {
                splitter.appendChild(doc.createTextNode(input.getSplitter()));
            }
            else {
                splitter.appendChild(doc.createTextNode("none"));
            }
            inputElement.appendChild(splitter);

            Element urlElement = doc.createElement("url");
            inputElement.appendChild(urlElement);
            urlElement.appendChild(doc.createTextNode(input.getUrl().toString()));
            if (input.isAutoComplete()) {
                urlElement.setAttribute("autocomplete", "true");
            }
        }

        // Splittable input
        Element inputElement = doc.createElement("input");
        rootElement.appendChild(inputElement);
        inputElement.setAttribute("id", splittableInput.getId());

        if (splittableInput.hasSplitter()) {
            Element splitter = doc.createElement("splitter");
            splitter.appendChild(doc.createTextNode(splittableInput.getSplitter()));
            inputElement.appendChild(splitter);
        }

        Element urlElement = doc.createElement("url");
        inputElement.appendChild(urlElement);
        urlElement.appendChild(doc.createTextNode(splittableInput.getUrl().toString()));
        // TODO can we use autocomplete for splittable input?
        if (splittableInput.isAutoComplete()) {
            urlElement.setAttribute("autocomplete", "true");
        }

        // Output
        Element outputElement = doc.createElement("output");
        rootElement.appendChild(outputElement);
        outputElement.setAttribute("id", jobOutput.getId());

        Element splitter = doc.createElement("reducer");
        splitter.appendChild(doc.createTextNode(jobOutput.getReducer()));
        if (jobOutput.isSequenceOutput()) {
            splitter.setAttribute("format", "sequence");
        }
        outputElement.appendChild(splitter);

        Element outUrlElement = doc.createElement("url");
        outputElement.appendChild(outUrlElement);
        outUrlElement.appendChild(doc.createTextNode(jobOutput.getUrl().toString()));
        
        // Hadoop config
        if (hadoopConfig.size() > 0) {
            Element hadoopElement = doc.createElement("hadoop");
            rootElement.appendChild(hadoopElement);
            
            for (Map.Entry<String, String> e : hadoopConfig.entrySet()) {
                Element hadoopConfElement = doc.createElement("config");
                hadoopElement.appendChild(hadoopConfElement);
                hadoopConfElement.setAttribute("key", e.getKey());
                hadoopConfElement.setTextContent(e.getValue());
            }
        }

        // Convert to string
        TransformerFactory factory = TransformerFactory.newInstance();
        Transformer transformer = null;
        try {
            transformer = factory.newTransformer();
        } catch (TransformerConfigurationException e) {
            System.err.println("Failed converting configuration to xml");
            e.printStackTrace();
            System.exit(1);
        }

        StringWriter sw = new StringWriter();
        StreamResult result = new StreamResult(sw);
        DOMSource source = new DOMSource(doc);
        try {
            transformer.transform(source, result);
        } catch (TransformerException e) {
            System.err.println("Failed converting configuration to xml");
            e.printStackTrace();
            System.exit(1);
        }

        return sw.toString();
    }

    /**
     * @return the command
     */
    public String getRawCommand() {

        return command;
    }

    /**
     * @return the command
     */
    public String getFinalCommand() {

        String finalCommand = command;

        if (splittableInput.getLocalPath().isEmpty()) // FIXME potential nullpointerexception
            throw new RuntimeException("Unable to generate command line: the splittable input local path is empty.");

        finalCommand = finalCommand.replaceAll("\\$\\{" + splittableInput.getId() + "\\}", splittableInput.getLocalPath()); // FIXME beware of special chars ($ etc) in replacement string

        for (JobInput in : staticInputs) {
            if (in.getLocalPath().isEmpty()) // FIXME potential nullpointerexception
                throw new RuntimeException("Unable to generate command line: the '" + in.getId() + "' input local path is empty.");

            finalCommand = finalCommand.replaceAll("\\$\\{" + in.getId() + "\\}", in.getLocalPath());
        }

        if (jobOutput.getLocalPath().isEmpty()) // FIXME handle other protocols // FIXME potential nullpointerexception
            throw new RuntimeException("Unable to generate command line: the '" + jobOutput.getId() + "' output local path is empty.");

        finalCommand = finalCommand.replaceAll("\\$\\{" + jobOutput.getId() + "\\}", jobOutput.getLocalPath());

        return finalCommand;
    }

    /**
     * @return
     */
    public HashSet<JobInput> getStaticInputs() {
        return staticInputs;
    }

    /**
     * @return
     */
    public JobOutput getJobOutput() {
        return jobOutput;
    }

    /**
     * @return
     */
    public JobInput getSplittableInput() {
        return splittableInput;
    }

    /**
     * @param id
     * @param path
     */
    public void setStaticInputLocalPath(String id, String path) {

        if (id == null || id.isEmpty())
            throw new RuntimeException("Invalid id for static input file");

        boolean found = false;

        for (JobInput in : staticInputs) {
            if (in.getId().equals(id)) {
                in.setLocalPath(path);
                found = true;
            }
        }

        if (!found)
            throw new RuntimeException("Could not find a static input file with id '" + id + "'");
    }

    /**
     * @return the hadoopConfig
     */
    public HashMap<String, String> getHadoopConfig() {
        return hadoopConfig;
    }

    /**
     * @param hadoopConfig the hadoopConfig to set
     */
    public void setHadoopConfig(HashMap<String, String> hadoopConfig) {
        this.hadoopConfig = hadoopConfig;
    }
}
