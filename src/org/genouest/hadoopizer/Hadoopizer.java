package org.genouest.hadoopizer;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.util.HashSet;
import java.util.Map;
import java.util.UUID;
import java.util.logging.Logger;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.xpath.XPathExpressionException;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.genouest.hadoopizer.mappers.GenericMapper;
import org.xml.sax.SAXException;

public class Hadoopizer {

    public static final Logger logger = Logger.getLogger("Hadoopizer");

    private static final String VERSION = "1.0";

    /**
     * @param args command line arguments
     * @throws IOException 
     * @throws SAXException 
     * @throws ParserConfigurationException 
     * @throws XPathExpressionException 
     */
    public static void main(String[] args) throws IOException {

        Configuration jobConf = new Configuration();

        args = new GenericOptionsParser(jobConf, args).getRemainingArgs();
        
        Options options = new Options();
        options.addOption("c", "config", true, "Path to a XML file describing the command to run");
        options.addOption("w", "work-dir", true, "HDFS url where temporary files will be placed. The directory must not already exist");
        options.addOption("h", "help", false, "Display help");
        options.addOption("v", "version", false, "Display version information");
        
        CommandLineParser parser = new GnuParser();
        CommandLine cmdLine = null;
        try {
            cmdLine = parser.parse(options, args);
        } catch (ParseException e) {
            System.err.println("Error parsing options");
            e.printStackTrace();
            help(options);
            System.exit(1);
        }
        
        if (cmdLine.hasOption("v"))
            version();

        if (cmdLine.hasOption("h") || !cmdLine.hasOption("c") || !cmdLine.hasOption("w"))
            help(options);

        // Load job config file
        File configFile = new File(cmdLine.getOptionValue("c"));
        logger.info("Reading config file: "+configFile.getAbsolutePath());

        if (!configFile.isFile()) {
            System.err.println("Couldn't read configuration file");
            System.exit(1);
        }

        JobConfig config = new JobConfig();
        try {
            config.load(configFile);
        } catch (FileNotFoundException e) {
            System.err.println("Couldn't read configuration file");
            e.printStackTrace();
            System.exit(1);
        }

        logger.info("Will execute the following command: " + config.getRawCommand());
        
        String hdfsWorkDir = cmdLine.getOptionValue("w");
        if (!hdfsWorkDir.endsWith(Path.SEPARATOR))
            hdfsWorkDir += Path.SEPARATOR;
        jobConf.set("hadoopizer.hdfs.tmp.dir", hdfsWorkDir);
        
        checkDirs(config, jobConf);

        setHadoopOptions(config, jobConf);
        
        // Prepare a hadoop job
        boolean success = false; 
        try {
            Job job = prepareJob(config, jobConf);
            success = job.waitForCompletion(true);
        } catch (IOException e) {
            System.err.println("Failed launching hadoop job");
            e.printStackTrace();
            System.exit(1);
        } catch (InterruptedException e) {
            System.err.println("Failed launching hadoop job");
            e.printStackTrace();
            System.exit(1);
        } catch (ClassNotFoundException e) {
            System.err.println("Failed launching hadoop job");
            e.printStackTrace();
            System.exit(1);
        }

        if (!success) {
            logger.severe("The hadoop job failed!");
            System.exit(1);
        }
    }
    
    private static void checkDirs(JobConfig config, Configuration jobConf) {
        Path cacheDir = new Path(jobConf.get("hadoopizer.hdfs.tmp.dir"));
        URI cacheUri = cacheDir.toUri();
        
        if (!cacheUri.getScheme().equalsIgnoreCase("hdfs")) {
            System.err.println("The working directory (-w option) must be an hdfs path ('" + cacheDir + "' given)");
            System.exit(1);
        }
        
        FileSystem fs = null;
        try {
            fs = cacheDir.getFileSystem(jobConf);
        } catch (IOException e) {
            System.err.println("Failed to connect to the working directory (-w option) ('" + cacheDir + "' given)");
            e.printStackTrace();
            System.exit(1);
        }
        
        try {
            if (fs.exists(cacheDir)) {
                System.err.println("The working directory (-w option) must not already exist before launching Hadoopizer ('" + cacheDir + "' given)");
                System.exit(1);
            }
        } catch (IOException e) {
            System.err.println("Failed checking the existence of the working directory (-w option) ('" + cacheDir + "' given)");
            e.printStackTrace();
            System.exit(1);
        }

        Path outputDir = new Path(config.getJobOutput().getUrl());
        try {
            FileSystem outputFs = outputDir.getFileSystem(jobConf);
            if (outputFs.exists(outputDir)) {
                System.err.println("The output directory must not already exist before launching Hadoopizer ('" + outputDir + "' given)");
                System.exit(1);
            }
        } catch (IOException e) {
            System.err.println("Failed checking the existence of the output directory ('" + outputDir + "' given)");
            System.exit(1);
        }
    }

    private static Job prepareJob(JobConfig config, Configuration jobConf) throws IOException {

        Path inputPath = new Path(config.getSplittableInput().getUrl());
        
        // Set job config
        try {
            jobConf.set("hadoopizer.job.config", config.dumpXml());
        } catch (ParserConfigurationException e) {
            System.err.println("Failed dumping configuration to xml.");
            e.printStackTrace();
            System.exit(1);
        }

        // Add static input files to distributed cache
        HashSet<JobInput> inputs = config.getStaticInputs();
        for (JobInput jobInput : inputs) {
            Path cacheDir = new Path(jobConf.get("hadoopizer.hdfs.tmp.dir"));
            Path hdfsBasePath = new Path(cacheDir.toString() + Path.SEPARATOR + jobInput.getId() + Path.SEPARATOR);

            if (jobInput.isAutoComplete()) {
                // We need to add to distributed cache all files with given prefix
                for (URI url : jobInput.getAllUrls(jobConf)) {
                    addToDistributedCache(jobInput.getId(), url, hdfsBasePath, jobConf);
                }
            }
            else {
                // No auto complete, simply add the given file to the distributed cache
                addToDistributedCache(jobInput.getId(), jobInput.getUrl(), hdfsBasePath, jobConf);
            }
        }

        DistributedCache.createSymlink(jobConf);

        // Create the job and its name
        Job job = new Job(jobConf, "Hadoopizer job"); // TODO find a better name (config?)

        job.setJarByClass(Hadoopizer.class);
        
        FileInputFormat<?, ?> iFormat = config.getSplittableInput().getFileInputFormat();
        job.setInputFormatClass(iFormat.getClass());
        
        FileOutputFormat<?, ?> oFormat = config.getJobOutput().getFileOutputFormat();
        job.setOutputFormatClass(oFormat.getClass());
        
        // Output compression if asked
        FileOutputFormat.setCompressOutput(job, config.getJobOutput().hasCompressor());
        if (config.getJobOutput().hasCompressor())
            FileOutputFormat.setOutputCompressorClass(job, config.getJobOutput().getCompressor());
        
        // Set input path
        FileInputFormat.setInputPaths(job, inputPath);

        // Set the Mapper class
        job.setMapperClass(GenericMapper.class);

        // Set the reducer class
        //job.setReducerClass(GenericReducer.class); // FIXME create a specific one if some outputs types can be reduced before writing

        // Set the output key class
        job.setOutputKeyClass(Text.class); // FIXME will we need another type some day?

        // Set the output value class
        job.setOutputValueClass(Text.class); // FIXME  will we need another type some day?

        // Set output path
        FileOutputFormat.setOutputPath(job, new Path(config.getJobOutput().getUrl()));

        return job;
    }

    /**
     * Add the hadoop options defined in the job config file
     * 
     * @param config
     * @param jobConf 
     */
    private static void setHadoopOptions(JobConfig config, Configuration jobConf) {
        for (Map.Entry<String, String> e : config.getHadoopConfig().entrySet()) {
            jobConf.set(e.getKey(), e.getValue());
        }

        // TODO move this elsewhere?
        Path cacheDir = new Path(jobConf.get("hadoopizer.hdfs.tmp.dir") + "temp_header_file.txt");
        jobConf.set("hadoopizer.temp.header.file", cacheDir.toString()); // TODO document this
    }
    
    // TODO distributed cache can also be used to distribute software
    // TODO compress the files maybe (archives are unarchived on the nodes)
    private static void addToDistributedCache(String fileId, URI uri, Path hdfsBasePath, Configuration jobConf) throws IOException {
        
        FileSystem fs = hdfsBasePath.getFileSystem(jobConf);
        Path localPath = new Path(uri);
        Path hdfsPath = new Path(hdfsBasePath.toString() + Path.SEPARATOR + localPath.getName());
        
        
        if (uri.getScheme().equalsIgnoreCase("file")) {
            logger.info("Adding file '" + uri + "' to distributed cache (" + hdfsPath + ")");
            fs.copyFromLocalFile(false, true, localPath, hdfsPath);
        }
        else if (uri.getScheme().equalsIgnoreCase("hdfs")) {
            logger.info("Adding file '" + uri + "' to distributed cache");
            Path cacheDir = new Path(jobConf.get("hadoopizer.hdfs.tmp.dir"));
            URI cacheUri = cacheDir.toUri();
            if (!uri.getHost().equalsIgnoreCase(cacheUri.getHost())) {
                // No transfer needed if on the same hdfs host
                // TODO Otherwise, download then copy? or just keep it like that? Need to test this
            }
            else {
                hdfsPath = localPath;
            }
        }
        else {
            // TODO stop everything, we don't know how to do!! support other protocols (s3? ssh? http? ftp?)
        }

        // Add a fragment to the uri: hadoop will automatically create a symlink in the work dir pointing to this file
        // Don't add the fragment to hdfsPath because it would be encoded in a strange way
        URI hdfsUri = URI.create(hdfsPath.toString() + "#static_data__" + fileId + "__" + localPath.getName());
        DistributedCache.addCacheFile(hdfsUri, jobConf);
    }
    
    private static void help(Options options) {

        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp("hadoop jar hadoopizer.jar", options);
        System.exit(0);
    }

    private static void version() {

        System.out.println("Hadoopizer " + VERSION);
        System.exit(0);
    }

    /**
     * Create a new temporary file.
     * @param directory parent directory of the temporary file to create
     * @param prefix prefix of the temporary file
     * @param suffix suffix of the temporary file
     * @return the new temporary file
     * @throws IOException if there is an error creating the temporary directory
     */
    public static File createTempFile(File directory, String prefix, String suffix) throws IOException {

        if (directory == null)
            throw new IOException("Parent directory is null");

        if (prefix == null)
            prefix = "";

        if (suffix == null)
            suffix = "";

        File tempFile;

        final int maxAttempts = 9;
        int attemptCount = 0;
        do {
            attemptCount++;
            if (attemptCount > maxAttempts)
                throw new IOException("Failed to create a unique temporary directory after " + maxAttempts + " attempts.");

            final String filename = prefix + UUID.randomUUID().toString() + suffix;
            tempFile = new File(directory, filename);
        } while (tempFile.exists());

        if (!tempFile.createNewFile())
            throw new IOException("Failed to create temp file " + tempFile.getAbsolutePath());

        return tempFile;
    }
}
