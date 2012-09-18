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
import java.util.Map.Entry;

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

import org.apache.hadoop.io.compress.BZip2Codec;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

public class JobConfig {

    private String command;
    private JobInput splitableInput;
    private HashSet<JobInput> staticInputs;
    private HashSet<JobOutput> jobOutputs;
    private HashMap<String, String> hadoopConfig;
    private String outputCompressor;
    private URI outputUrl;
    
    /**
     * List of allowed compression codecs
     */
    private static final HashMap<String, Class<? extends CompressionCodec>> allowedCodecs = new HashMap<String, Class<? extends CompressionCodec>>();
    static {
        allowedCodecs.put("gzip", GzipCodec.class);
        allowedCodecs.put("bzip2", BZip2Codec.class);
    }

    public JobConfig() {

        staticInputs = new HashSet<JobInput>();
        jobOutputs = new HashSet<JobOutput>();
        hadoopConfig = new HashMap<String, String>();
    }

    /**
     * Get the url where the output data will be saved, as declared in the config
     * 
     * @return the outputUrl
     */
    public URI getOutputUrl() {
        return outputUrl;
    }

    /**
     * Set the url where the output data will be saved, as declared in the config
     * 
     * @param url the outputUrl to set
     */
    public void setOutputUrl(URI outputUrl) {
        this.outputUrl = outputUrl;
    }

    /**
     * Validates an XML config file using an xml schema
     * Requires the latest xalan.jar xercesImpl.jar and xml-apis.jar (implementing XML Schema v1.1) in the java.endorsed.dirs
     * 
     * @param configFile a config file to validate
     * @return true if the given xml is valid
     * @throws FileNotFoundException
     */
    public boolean validateXml(File configFile) throws FileNotFoundException {

        FileInputStream fis = new FileInputStream(configFile);
        return validateXml(fis);
    }

    /**
     * Validates an XML config file using an xml schema
     * Requires the latest xalan.jar xercesImpl.jar and xml-apis.jar (implementing XML Schema v1.1) in the java.endorsed.dirs
     * 
     * @param configContent a xml string to validate
     * @return true if the given xml is valid
     */
    public boolean validateXml(String configContent) {

        InputStream is = new ByteArrayInputStream(configContent.getBytes());
        return validateXml(is);
    }
    
    /**
     * Validates an XML config file using an xml schema
     * Requires the latest xalan.jar xercesImpl.jar and xml-apis.jar (implementing XML Schema v1.1) in the java.endorsed.dirs
     * 
     * @param configContent InputStream containing the xml data to validate
     * @return true if the given xml is valid
     */
    public boolean validateXml(InputStream configContent) {
        
        StreamSource[] schemaDocuments = new StreamSource[] {new StreamSource("resources/jobconfig.xsd")};
        Source instanceDocument = new StreamSource(configContent);
        
        SchemaFactory sf = SchemaFactory.newInstance("http://www.w3.org/XML/XMLSchema/v1.1");
        Schema s;
        try {
            s = sf.newSchema(schemaDocuments);
            Validator v = s.newValidator();
            v.validate(instanceDocument);
        } catch (SAXException e) {
            Hadoopizer.logger.warning("Invalid config XML: "+e.getMessage());
            e.printStackTrace();
            return false;
        } catch (IOException e) {
            Hadoopizer.logger.warning("Failed reading XML: "+e.getMessage());
            e.printStackTrace();
            return false;
        }
        
        return true;
    }
    
    /**
     * Load the conf configuration from an xml file
     * 
     * @param configFile the xml file to load
     * @throws FileNotFoundException
     */
    public void load(File configFile) throws FileNotFoundException {

        FileInputStream fis = new FileInputStream(configFile);
        load(fis);
    }

    /**
     * Load the conf configuration from an xml String
     * 
     * @param configContent the xml String
     */
    public void load(String configContent) {

        InputStream is = new ByteArrayInputStream(configContent.getBytes());
        load(is);
    }
    
    /**
     * Load the conf configuration from an InputStream
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
        
        if (inputs.getLength() < 1) {
            System.err.println("The config file should contain at least one 'input' element");
            System.exit(1);
        }

        for (int i = 0; i < inputs.getLength(); i++) {
            Element input = (Element) inputs.item(i);
            
            if (!input.hasAttribute("id")) {
                System.err.println("Each 'input' element should have an 'id' attribute");
                System.exit(1);
            }
            
            boolean isSplitable = input.hasAttribute("split") && (input.getAttribute("split").compareToIgnoreCase("true") == 0);
            if (isSplitable) {
                JobInput jobInput = new SplitableJobInput(input.getAttribute("id"));
                jobInput.loadXml(input);
                splitableInput = jobInput;
            }
            else {
                JobInput jobInput = new StaticJobInput(input.getAttribute("id"));
                jobInput.loadXml(input);
                staticInputs.add(jobInput);
            }
        }

        // Check that there are some inputs
        if (splitableInput == null) {
            System.err.println("The config file should contain exactly one 'input' element with a splitter");
            System.exit(1);
        }
        
        // Get the outputs
        NodeList outputs = null;
        Element outputsElem = null;
        try {
            XPathExpression expr = xpath.compile("/job/outputs");
            outputs = (NodeList) expr.evaluate(doc, XPathConstants.NODESET);
            outputsElem = (Element) outputs.item(0);
        } catch (XPathExpressionException e) {
            e.printStackTrace();
            System.exit(1);
        }
        
        if (outputs.getLength() != 1) {
            System.err.println("The config file should contain exactly one 'outputs' element");
            System.exit(1);
        }
        
        // Output compressor
        Element compressor = (Element) outputsElem.getElementsByTagName("compressor").item(0);
        if (compressor != null) {
            String codec = compressor.getTextContent();
            if (isOutputCompressorSupported(codec)) {
                Hadoopizer.logger.info("Compressing the output with " + codec);
                setOutputCompressor(compressor.getTextContent());
            }
            else {
                System.err.println("Unsupported output compressor '" + codec + "' (allowed: " + getSupportedOutputCompressor() + ")");
                System.exit(1);
            }
        }
        
        // Output url
        Node urlNode = outputsElem.getElementsByTagName("url").item(0);
        String url = "";
        if (urlNode != null) {
            url = urlNode.getTextContent();
            if (url.startsWith("/"))
                url = "file:" + url;
        }
        else {
            System.err.println("The config file should contain an 'url' element in 'outputs' element");
            System.exit(1);
        }
        
        // Output url
        try {
            setOutputUrl(new URI(url));
        } catch (URISyntaxException e) {
            System.err.println("Wrong URI format in config file: "+url);
            e.printStackTrace();
            System.exit(1);
        }
        
        // Get the outputs
        NodeList outs = outputsElem.getElementsByTagName("output");

        if (outs.getLength() < 1) {
            System.err.println("The config file should contain at least one 'output' element in 'outputs' element");
            System.exit(1);
        }

        for(int i = 0; i < outs.getLength(); i++) {
            Element output = (Element) outs.item(i);
            
            if (!output.hasAttribute("id") || !output.hasAttribute("reducer")) {
                System.err.println("Each 'output' element should have an 'id' and a 'reducer' attribute");
                System.exit(1);
            }

            JobOutput jobOutput = new JobOutput(output.getAttribute("id"));

            // Reducer
            jobOutput.setReducerId(output.getAttribute("reducer"));
            if (output.hasAttribute("sequence") && output.getAttribute("sequence").equalsIgnoreCase("true")) {
                // The output needs to be stored as hadoop SequenceFile for reuse in a future hadoop conf
                jobOutput.setSaveAsSequence(true);
            }
            
            jobOutputs.add(jobOutput);
            
            Hadoopizer.logger.info("Using reducer '"+jobOutput.getReducerId()+"' for output '"+jobOutput.getId()+"' ("+getOutputUrl()+")");
        }
        
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
     * Generate and XML representation of the current conf config
     * 
     * @return an XML string representing the conf config
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
            Element inputElement = input.dumpXml(doc);
            rootElement.appendChild(inputElement);
        }

        // Splitable input
        Element inputElement = splitableInput.dumpXml(doc);
        rootElement.appendChild(inputElement);

        // Output
        Element outputsElement = doc.createElement("outputs");
        rootElement.appendChild(outputsElement);
        for (JobOutput jobOutput : jobOutputs) {
            Element outputElement = doc.createElement("output");
            outputsElement.appendChild(outputElement);
            outputElement.setAttribute("id", jobOutput.getId());
            outputElement.setAttribute("reducer", jobOutput.getReducerId());
            if (jobOutput.isSaveAsSequence()) {
                outputElement.setAttribute("sequence", "true");
            }
        }

        if (hasOutputCompressor()) {
            Element compressor = doc.createElement("compressor");
            compressor.appendChild(doc.createTextNode(getOutputCompressorName()));
            outputsElement.appendChild(compressor);
        }

        Element outUrlElement = doc.createElement("url");
        outputsElement.appendChild(outUrlElement);
        outUrlElement.appendChild(doc.createTextNode(getOutputUrl().toString()));
        
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

        finalCommand = splitableInput.prepareCommand(finalCommand);

        for (JobInput in : staticInputs) {
            finalCommand = in.prepareCommand(finalCommand);
        }

        for (JobOutput jobOutput : jobOutputs) {
            if (jobOutput.getLocalPath().isEmpty()) // TODO handle other protocols (eg upload output to s3, hdfs, whatever)
                throw new RuntimeException("Unable to generate command line: the '" + jobOutput.getId() + "' output local path is empty.");

            finalCommand = finalCommand.replaceAll("\\$\\{" + jobOutput.getId() + "\\}", jobOutput.getLocalPath());
        }

        return finalCommand;
    }

    /**
     * Get the list of JobInput object representing static input files used (but not modified) by the conf
     * 
     * @return a Set of JobInput objects
     */
    public HashSet<JobInput> getStaticInputs() {
        return staticInputs;
    }

    /**
     * Get the JobOutput object representing the output file of the conf
     * 
     * @return a JobOutput object
     */
    public HashSet<JobOutput> getJobOutputs() {
        return jobOutputs;
    }

    /**
     * Get the JobInput object representing the input file of the conf which will be splitted by the hadoop framework
     * 
     * @return a JobInput object
     */
    public JobInput getSplitableInput() {
        return splitableInput;
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
                ((StaticJobInput) in).setLocalPath(path);
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

    /**
     * Get the name of the output compressor to use
     * 
     * @return The name of the output compressor to use
     */
    public String getOutputCompressorName() {
        return outputCompressor;
    }
    
    /**
     * Get the output compressor class
     * 
     * @return the output compressor class
     */
    public Class<? extends CompressionCodec> getOutputCompressor() {
        if (allowedCodecs.containsKey(outputCompressor))
            return allowedCodecs.get(outputCompressor);
        
        return null;
    }

    /**
     * Set the name of the output compressor to use
     * 
     * @param compressor the output compressor to set
     */
    public void setOutputCompressor(String outputCompressor) {
        this.outputCompressor = outputCompressor;
    }

    /**
     * Is there an output compressor for this output
     * 
     * @return true if an output compressor is set
     */
    public boolean hasOutputCompressor() {
        return allowedCodecs.containsKey(outputCompressor);
    }
    
    /**
     * Is the given output compressor name is supported
     * 
     * @return true if given output compressor name is supported
     */
    public static boolean isOutputCompressorSupported(String outputCompressor) {
        return allowedCodecs.containsKey(outputCompressor);
    }

    /**
     * Get a list of supported output compressors
     * 
     * @return a comma separated list of supported output compressors
     */
    public static String getSupportedOutputCompressor() {
        String sup = "";
        
        for (Entry<String, Class<? extends CompressionCodec>> e : allowedCodecs.entrySet()) {
            sup += e.getKey() + " ";
        }
        
        return sup;
    }

    /**
     * Check that all variables declared in the xml are used in the command line
     */
    public void checkVariables() {
        
        for (JobInput in : getStaticInputs()) {
            if (!command.contains("${" + in.getId() + "}")) {
                throw new RuntimeException("Input file '${" + in.getId() + "}' not found in the command line: '" + command + "'");
            }
        }
        
        for (JobOutput out : jobOutputs) {
            if (!command.contains("${" + out.getId() + "}")) {
                throw new RuntimeException("Output file '${" + out.getId() + "}' not found in the command line: '" + command + "'");
            }
        }
        
        int numSplits = ((SplitableJobInput) getSplitableInput()).getFiles().size();
        String splitId = getSplitableInput().getId();
        for (int i = 1; i <= numSplits; i++) {
            
            String suffix = "";
            if (((SplitableJobInput) getSplitableInput()).needJoin())
                suffix = "#" + i;
            
            if (!command.contains("${" + splitId + suffix + "}")) {
                throw new RuntimeException("Input file '${" + splitId + suffix + "}' not found in the command line: '" + command + "'");
            }
        }
        
    }
}
