package org.genouest.hadoopizer;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.util.HashSet;
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
        options.addOption("w", "work-dir", true, "HDFS url where temporary files will be placed. The directory must not exist"); // FIXME this is not required if data is already in hdfs
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

        if (cmdLine.hasOption("h") || !cmdLine.hasOption("c") || ! cmdLine.hasOption("w"))
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
        
        jobConf.set("hadoopizer.hdfs.cache.dir", cmdLine.getOptionValue("w"));
        checkCacheDir(jobConf);

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
    
    private static void checkCacheDir(Configuration jobConf) {
        Path cacheDir = new Path(jobConf.get("hadoopizer.hdfs.cache.dir"));
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
                System.err.println("The working directory (-w option) must not already exist befoe launching Hadoopizer ('" + cacheDir + "' given)");
                System.exit(1);
            }
        } catch (IOException e) {
            System.err.println("Failed to check the existence of the working directory (-w option) ('" + cacheDir + "' given)");
            e.printStackTrace();
            System.exit(1);
        }
    }

    private static Job prepareJob(JobConfig config, Configuration jobConf) throws IOException {

        Path inputPath = new Path(config.getSplittableInput().getUrl());

        jobConf.set("mapred.child.java.opts", "-Xmx1024m"); // FIXME make this configurable?
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
            Path hdfsBasePath = new Path("hdfs://192.168.2.20/data/tmp/" + jobInput.getId() + Path.SEPARATOR); // FIXME ask the path from command line arg, and check that it doesn't already exist
            FileSystem fs = hdfsBasePath.getFileSystem(jobConf);

            // TODO distributed cache can also be used to distribute software

            if (jobInput.isAutoComplete()) {
                // We need to add to distributed cache all files with given prefix
                for (URI url : jobInput.getAllUrls()) {
                    // TODO compress the files maybe (archives are unarchived on the nodes)
                    Path localPath = new Path(url);
                    Path hdfsPath = new Path(hdfsBasePath.toString() + Path.SEPARATOR + localPath.getName());// FIXME make it configurable/variable somehow
                    logger.info("adding file '" + url + "' to distributed cache (" + hdfsPath + ")");
                    if (!fs.exists(hdfsPath)) { // FIXME just for debugging, use hdfs in config file
                        // Avoid recopying if already existing
                        fs.copyFromLocalFile(false, true, localPath, hdfsPath); // FIXME make it work for all protocols
                    }

                    // Add a fragment to the uri: hadoop will automatically create a symlink in the work dir pointing to this file
                    // Don't add the fragment to hdfsPath because it would be encoded in a strange way
                    URI hdfsUri = URI.create(hdfsPath.toString() + "#static_data__" + jobInput.getId() + "__" + localPath.getName()); // FIXME check name collisions
                    DistributedCache.addCacheFile(hdfsUri, jobConf);
                }
            }
            else {
                // No auto complete, simply add the given file to the distributed cache
                Path localPath = new Path(jobInput.getUrl());
                Path hdfsPath = new Path(hdfsBasePath.toString() + Path.SEPARATOR + localPath.getName()); // FIXME make it configurable/variable somehow
                logger.info("adding file '" + jobInput.getUrl() + "' to distributed cache (" + hdfsPath + ")");
                fs.copyFromLocalFile(false, true, localPath, hdfsPath); // FIXME make it work for all protocols
                // TODO avoid recopying if already existing

                // Add a fragment to the uri: hadoop will automatically create a symlink in the work dir pointing to this file
                // Don't add the fragment to hdfsPath because it would be encoded in a strange way
                URI hdfsUri = URI.create(hdfsPath.toString() + "#static_data__" + jobInput.getId() + "__" + localPath.getName()); // FIXME choose a better fragment (beware of name collisions)
                DistributedCache.addCacheFile(hdfsUri, jobConf);
            }
        }

        DistributedCache.createSymlink(jobConf);

        // Create the job and its name
        Job job = new Job(jobConf, "Hadoopizer job");

        job.setJarByClass(Hadoopizer.class);
        
        FileInputFormat<?, ?> iFormat = config.getSplittableInput().getFileInputFormat();
        job.setInputFormatClass(iFormat.getClass());
        
        FileOutputFormat<?, ?> oFormat = config.getJobOutput().getFileOutputFormat();
        job.setOutputFormatClass(oFormat.getClass());
        
        // Set input path
        FileInputFormat.setInputPaths(job, inputPath);

        // Set the Mapper class
        job.setMapperClass(GenericMapper.class);

        // Set the reducer class
        //job.setReducerClass(GenericReducer.class); // FIXME create a specific one if some outputs types can be reduced before writing

        // Set the output key class
        job.setOutputKeyClass(Text.class); // FIXME use the correct type depending on config.xml

        // Set the output value class
        job.setOutputValueClass(Text.class);// FIXME use the correct type depending on config.xml

        // Set output path
        FileOutputFormat.setOutputPath(job, new Path(config.getJobOutput().getUrl()));

        return job;
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
}
