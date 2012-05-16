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
import javax.xml.transform.Source;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerConfigurationException;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import javax.xml.transform.stream.StreamSource;
import javax.xml.validation.Schema;
import javax.xml.validation.SchemaFactory;
import javax.xml.validation.Validator;
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
     * Validates an XML config file using an xml schema
     * Requires the latest xalan.jar xercesImpl.jar and xml-apis.jar (implementing XML Schema v1.1) in the java.endorsed.dirs
     * 
     * @param configFile a config file to validate
     * @throws FileNotFoundException
     */
    public boolean validateXml(File configFile) throws FileNotFoundException {

        FileInputStream fis = new FileInputStream(configFile);
        return validateXml(fis);
    }

    /**
     * @param configContent
     */
    public boolean validateXml(String configContent) {

        InputStream is = new ByteArrayInputStream(configContent.getBytes());
        return validateXml(is);
    }
    
    /**
     * @param configContent
     * @return
     */
    public boolean validateXml(InputStream configContent) {
        
        // TODO find a way to embed the java.endorsed.dirs definition in the jar, if possible. For the moment, we need to launch with -Djava.endorsed.dirs=lib/endorsed
        
        StreamSource[] schemaDocuments = new StreamSource[] {new StreamSource("resources/jobconfig.xsd")};
        Source instanceDocument = new StreamSource(configContent);
        
        SchemaFactory sf = SchemaFactory.newInstance("http://www.w3.org/XML/XMLSchema/v1.1");
        Schema s;
        try {
            s = sf.newSchema(schemaDocuments);
            Validator v = s.newValidator();
            v.validate(instanceDocument);
        } catch (SAXException e) {
            System.err.println("Failed loading XML Schema file for config file");
            e.printStackTrace();
            System.exit(1);
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(1);
        }
        return true;
    }
    
    /**
     * Load the job configuration from an xml file
     * 
     * @param configFile the xml file to load
     * @throws FileNotFoundException
     */
    public void load(File configFile) throws FileNotFoundException {

        FileInputStream fis = new FileInputStream(configFile);
        load(fis);
    }

    /**
     * Load the job configuration from an xml String
     * 
     * @param configContent the xml String
     */
    public void load(String configContent) {

        InputStream is = new ByteArrayInputStream(configContent.getBytes());
        load(is);
    }
    
    /**
     * Load the job configuration from an InputStream
     * 
     * @param configContent an InputStream object containing XML data
     */
    public void load(InputStream configContent) {
        
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
            boolean isSplittable = input.getElementsByTagName("splitter").item(0).getTextContent().equalsIgnoreCase("none");
            if (!isSplittable) {
                jobInput.setSplitterId(input.getElementsByTagName("splitter").item(0).getTextContent());
            }

            String url = input.getElementsByTagName("url").item(0).getTextContent();
            if (url.startsWith("/"))
                url = "file:" + url;
            try {
                jobInput.setUrl(new URI(url));
            } catch (URISyntaxException e) {
                System.err.println("Wrong URI format in config file: "+input.getElementsByTagName("url").item(0).getTextContent());
                e.printStackTrace();
                System.exit(1);
            }

            Element urlEl = (Element) input.getElementsByTagName("url").item(0);

            if (!isSplittable && urlEl.hasAttribute("autocomplete") && urlEl.getAttribute("autocomplete").equalsIgnoreCase("true")) {
                // Using autocomplete on splittable input makes no sense
                jobInput.setAutoComplete(true);
            }

            if (jobInput.hasSplitter()) {
                Hadoopizer.logger.info("Using splitter '"+jobInput.getSplitterId()+"' for input '"+jobInput.getId()+"' ("+jobInput.getUrl()+")");
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
        // Output compressor
        Element compressor = (Element) output.getElementsByTagName("compressor").item(0);
        if (compressor != null) {
            String codec = compressor.getTextContent();
            if (JobOutput.isCompressorSupported(codec)) {
                Hadoopizer.logger.info("Compressing the output with " + codec);
                jobOutput.setCompressor(codec);
            }
            else {
                System.err.println("Unsupported output compressor '" + codec + "' (allowed: " + JobOutput.getSupportedCompressor() + ")");
                System.exit(1);
            }
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
                Hadoopizer.logger.info("Hadoop config key found: '"+hConf.getAttribute("key")+"' -> '"+hConf.getTextContent()+"'");
                hadoopConfig.put(hConf.getAttribute("key"), hConf.getTextContent());
            }
        }
    }

    /**
     * Generate and XML representation of the current job config
     * 
     * @return an XML string representing the job config
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
                splitter.appendChild(doc.createTextNode(input.getSplitterId()));
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
            splitter.appendChild(doc.createTextNode(splittableInput.getSplitterId()));
            inputElement.appendChild(splitter);
        }

        Element urlElement = doc.createElement("url");
        inputElement.appendChild(urlElement);
        urlElement.appendChild(doc.createTextNode(splittableInput.getUrl().toString()));
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

        if (jobOutput.hasCompressor()) {
            Element compressor = doc.createElement("compressor");
            compressor.appendChild(doc.createTextNode(jobOutput.getCompressorName()));
            outputElement.appendChild(compressor);
        }

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
     * Get the command line as written in the original xml file (variables are not replaced)
     * 
     * @return the command line
     */
    public String getRawCommand() {

        return command;
    }

    /**
     * Get the command line with all variables replaced by local path, ready to execute on the corresponding node
     * 
     * @return the command, ready to execute
     */
    public String getFinalCommand() {

        String finalCommand = command;

        if (splittableInput.getLocalPath().isEmpty())
            throw new RuntimeException("Unable to generate command line: the splittable input local path is empty.");

        finalCommand = finalCommand.replaceAll("\\$\\{" + splittableInput.getId() + "\\}", splittableInput.getLocalPath());

        for (JobInput in : staticInputs) {
            if (in.getLocalPath().isEmpty())
                throw new RuntimeException("Unable to generate command line: the '" + in.getId() + "' input local path is empty.");

            finalCommand = finalCommand.replaceAll("\\$\\{" + in.getId() + "\\}", in.getLocalPath());
        }

        if (jobOutput.getLocalPath().isEmpty()) // FIXME handle other protocols (eg upload output to s3, hdfs, whatever)
            throw new RuntimeException("Unable to generate command line: the '" + jobOutput.getId() + "' output local path is empty.");

        finalCommand = finalCommand.replaceAll("\\$\\{" + jobOutput.getId() + "\\}", jobOutput.getLocalPath());

        return finalCommand;
    }

    /**
     * Get the list of JobInput object representing static input files used (but not modified) by the job
     * 
     * @return a Set of JobInput objects
     */
    public HashSet<JobInput> getStaticInputs() {
        return staticInputs;
    }

    /**
     * Get the JobOutput object representing the output file of the job
     * 
     * @return a JobOutput object
     */
    public JobOutput getJobOutput() {
        return jobOutput;
    }

    /**
     * Get the JobInput object representing the input file of the job which will be splitted by the hadoop framework
     * 
     * @return a JobInput object
     */
    public JobInput getSplittableInput() {
        return splittableInput;
    }

    /**
     * Associate a local, node-specific path to a static input file
     * 
     * @param id the id of the static input file
     * @param path the node-specific path to the file
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
     * Get the list of hadoop config key/value pairs
     * 
     * @return the hadoopConfig
     */
    public HashMap<String, String> getHadoopConfig() {
        return hadoopConfig;
    }

    /**
     * Set the list of hadoop config key/value pairs
     * 
     * @param hadoopConfig the hadoopConfig to set
     */
    public void setHadoopConfig(HashMap<String, String> hadoopConfig) {
        this.hadoopConfig = hadoopConfig;
    }
}
