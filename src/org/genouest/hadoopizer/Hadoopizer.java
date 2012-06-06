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
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.genouest.hadoopizer.input.HadoopizerInputFormat;
import org.genouest.hadoopizer.mappers.GenericMapper;
import org.genouest.hadoopizer.output.HadoopizerOutputFormat;

public class Hadoopizer {

    public static final Logger logger = Logger.getLogger("Hadoopizer"); // Logger
    private static final String VERSION = "1.0"; // Version number of Hadoopizer

    private Configuration jobConf = new Configuration(); // Hadoop configuration object
    private JobConfig config = new JobConfig(); // Hadoopizer configuration object
    private CommandLine cmdLine; // Command line args
    
    /**
     * @param args command line arguments
     * @throws IOException
     */
    public static void main(String[] args) throws IOException {

        Hadoopizer app = new Hadoopizer();
        
        // Load command line args + xml config
        app.loadConfig(args);

        // Load hadoop specific options
        app.setHadoopOptions();
        
        // Check temp and output dir presence
        app.checkDirs();
        
        // Prepare a hadoop job
        boolean success = false; 
        try {
            Job job = app.prepareJob();
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
    
    /**
     * Parse the command line arguments and load data from xml config file
     * 
     * @param args command line arguments
     * @throws IOException 
     */
    private void loadConfig(String[] args) throws IOException {

        // Parse hadoop specific args
        args = new GenericOptionsParser(jobConf, args).getRemainingArgs();
        
        // Define our options
        Options options = new Options();
        options.addOption("c", "config", true, "Path to a XML file describing the command to run");
        options.addOption("w", "work-dir", true, "HDFS url where temporary files will be placed. The directory must not already exist");
        options.addOption("b", "binaries", true, "Archive containing the binaries to execute. The archive is unzipped on each node in a 'binaries' directory in the work dir.");
        options.addOption("h", "help", false, "Display help");
        options.addOption("v", "version", false, "Display version information");
        
        // Parse our options
        CommandLineParser parser = new GnuParser();
        try {
            cmdLine = parser.parse(options, args);
        } catch (ParseException e) {
            System.err.println("Error parsing options");
            e.printStackTrace();
            help(options);
            System.exit(1);
        }
        
        // Version and help options
        if (cmdLine.hasOption("v"))
            version();

        if (cmdLine.hasOption("h") || !cmdLine.hasOption("c") || !cmdLine.hasOption("w"))
            help(options);

        // Load job config file from xml file
        File configFile = new File(cmdLine.getOptionValue("c"));
        if (!configFile.isFile()) {
            System.err.println("Could not read the configuration file: "+configFile.getAbsolutePath());
            System.exit(1);
        }
        logger.info("Reading config file: "+configFile.getAbsolutePath());
        
        // We could validate the xml but it introduces a dependency on endorsed lib for xml schema 1.1
        /*try {
            config.validateXml(configFile);
        } catch (FileNotFoundException e) {
            System.err.println("Couldn't read configuration file");
            e.printStackTrace();
            System.exit(1);
        }*/
        
        // The xml looks ok, try to load it
        try {
            config.load(configFile);
        } catch (FileNotFoundException e) {
            System.err.println("Could not read the configuration file: "+configFile.getAbsolutePath());
            e.printStackTrace();
            System.exit(1);
        }
        
        // Define the HDFS work dir
        String hdfsWorkDir = cmdLine.getOptionValue("w");
        if (!hdfsWorkDir.endsWith(Path.SEPARATOR))
            hdfsWorkDir += Path.SEPARATOR;
        jobConf.set("hadoopizer.hdfs.tmp.dir", hdfsWorkDir);
    }

    /**
     * Add binary archive given by command line argument (if any) to ditributed cache
     * 
     * @param optionValue
     * @throws IOException 
     */
    private void loadBinaries() throws IOException {

        if (!cmdLine.hasOption("b")) {
            return;
        }

        String path = cmdLine.getOptionValue("b");
        
        File arch = new File(path);
        if (!arch.exists() || !arch.isFile()) {
            System.err.println("Could not find binaries (-b option): " + path);
            System.exit(1);
        }
        
        URI archUri = arch.toURI();
        Path archPath = new Path(archUri);
        
        Path hdfsPath = new Path(jobConf.get("hadoopizer.hdfs.tmp.dir") + Path.SEPARATOR + jobConf.get("hadoopizer.binaries.link.name") + Path.SEPARATOR + archPath.getName());
        FileSystem fs = hdfsPath.getFileSystem(jobConf);
        
        if (archUri.getScheme().equalsIgnoreCase("file")) {
            fs.copyFromLocalFile(false, true, archPath, hdfsPath);
        }
        else {
            // TODO compatibility with other protocols: s3, http, ftp, ssh, hdfs
        }
        
        URI hdfsUri = URI.create(hdfsPath.toString() + "#" + jobConf.get("hadoopizer.binaries.link.name"));
        DistributedCache.addCacheArchive(hdfsUri, jobConf);
    }

    /**
     * Check that hdfs work dir and the output dir are valid ones.
     * Exit the application if it is not the case.
     */
    private void checkDirs() {
        
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

    /**
     * Prepare a ready to use job object based on the config loaded from command line args
     * and xml config file.
     * 
     * @return a job object ready for execution
     * @throws IOException
     */
    private Job prepareJob() throws IOException {

        Path inputPath = new Path(config.getSplittableInput().getUrl());
        
        // Add static input files to distributed cache
        HashSet<JobInput> inputs = config.getStaticInputs();
        for (JobInput jobInput : inputs) {
            Path cacheDir = new Path(jobConf.get("hadoopizer.hdfs.tmp.dir"));
            Path hdfsBasePath = new Path(cacheDir.toString() + Path.SEPARATOR + "static_data" + Path.SEPARATOR + jobInput.getId() + Path.SEPARATOR);

            // We need to add to distributed cache static file(s)
            for (URI url : jobInput.getAllUrls(jobConf)) {
                addToDistributedCache(jobInput.getId(), url, hdfsBasePath);
            }
        }
        
        // If needed, put the binaries in the distributed cache
        loadBinaries();

        // Ensure the files placed in the distributed cache will have symlinks in the work dir
        DistributedCache.createSymlink(jobConf);

        // Create the job and its name
        Job job = new Job(jobConf, jobConf.get("hadoopizer.job.name"));

        job.setJarByClass(Hadoopizer.class);
        
        // Define input and output data format
        HadoopizerInputFormat<?, ?> iFormat = config.getSplittableInput().getFileInputFormat();
        job.setInputFormatClass(iFormat.getClass());
        
        HadoopizerOutputFormat<?, ?> oFormat = config.getJobOutput().getFileOutputFormat();
        job.setOutputFormatClass(oFormat.getClass());
        
        // Output compression if asked
        FileOutputFormat.setCompressOutput(job, config.getJobOutput().hasCompressor());
        if (config.getJobOutput().hasCompressor())
            FileOutputFormat.setOutputCompressorClass(job, config.getJobOutput().getCompressor());
        
        // Set input path
        FileInputFormat.setInputPaths(job, inputPath);

        // Set the Mapper class
        job.setMapperClass(GenericMapper.class); // TODO rename genericmapper and allow to use different mappers

        // Set the reducer class
        //job.setReducerClass(GenericReducer.class); // TODO create a specific one if some outputs types can be reduced before writing

        // Set the output key class
        job.setOutputKeyClass(oFormat.getOutputKeyClass());

        // Set the output value class
        job.setOutputValueClass(oFormat.getOutputValueClass());
        
        // Set output path
        FileOutputFormat.setOutputPath(job, new Path(config.getJobOutput().getUrl()));

        return job;
    }

    /**
     * Load the hadoop options defined in the job config file
     */
    private void setHadoopOptions() {

        // Add the job config
        try {
            jobConf.set("hadoopizer.job.config", config.dumpXml());
        } catch (ParserConfigurationException e) {
            System.err.println("Failed dumping configuration to xml.");
            e.printStackTrace();
            System.exit(1);
        }
        
        // First define some default settings
        Path cacheDir = new Path(jobConf.get("hadoopizer.hdfs.tmp.dir")); // Defined from command line
        jobConf.set("hadoopizer.temp.input.header.file", cacheDir.toString() + Path.SEPARATOR + "temp_input_header_file.txt");
        jobConf.set("hadoopizer.temp.output.header.file", cacheDir.toString() + Path.SEPARATOR  + "temp_output_header_file.txt");
        jobConf.set("hadoopizer.binaries.link.name", "binaries");
        jobConf.set("hadoopizer.job.name", "Hadoopizer job");
        jobConf.set("hadoopizer.shell.interpreter", "#!/bin/bash");
        jobConf.set("hadoopizer.static.data.link.prefix", "static_data__");
        
        // Then load other options from job file (overriding if needed)
        for (Map.Entry<String, String> e : config.getHadoopConfig().entrySet()) {
            jobConf.set(e.getKey(), e.getValue());
        }
    }
    
    /**
     * Add an input file to the distributed cache.
     * It is possible to call this method with different uris for the same file id.
     * 
     * @param fileId the file id as defined in the xml config file
     * @param uri the uri of the file to add to distributedcache
     * @param hdfsBasePath HDFS base path were the file will be placed directly (no subdir will be created)
     * @throws IOException
     */
    private void addToDistributedCache(String fileId, URI uri, Path hdfsBasePath) throws IOException {
        
        FileSystem fs = hdfsBasePath.getFileSystem(jobConf);
        Path localPath = new Path(uri);
        Path hdfsPath = new Path(hdfsBasePath.toString() + Path.SEPARATOR + localPath.getName());
        
        if (uri.getScheme().equalsIgnoreCase("file")) {
            logger.info("Adding file '" + uri + "' to distributed cache (" + hdfsPath + ")");
            fs.copyFromLocalFile(false, true, localPath, hdfsPath);
        }
        else if (uri.getScheme().equalsIgnoreCase("hdfs")) {
            logger.info("Adding file '" + uri + "' to distributed cache");
            hdfsPath = localPath;
        }
        else {
            // TODO support other protocols (s3? ssh? http? ftp?)
            System.err.println("Unsupported URI scheme: " + uri.getScheme() + " (in " + uri + ")");
            System.exit(1);
        }

        // Add a fragment to the uri: hadoop will automatically create a symlink in the work dir pointing to this file
        // Don't add the fragment to hdfsPath because it would be encoded in a strange way
        URI hdfsUri = URI.create(hdfsPath.toString() + "#" + jobConf.get("hadoopizer.static.data.link.prefix") + fileId + "__" + localPath.getName());
        DistributedCache.addCacheFile(hdfsUri, jobConf);
    }
    
    /**
     * Show usage information and exit
     * 
     * @param options options available for hadoopizer
     */
    private static void help(Options options) {

        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp("hadoop jar hadoopizer.jar", options);
        System.exit(0);
    }

    /**
     * Display the version number and exit
     */
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
