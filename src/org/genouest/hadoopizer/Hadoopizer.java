package org.genouest.hadoopizer;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.util.HashSet;
import java.util.logging.Logger;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.xpath.XPathExpressionException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.GenericOptionsParser;
import org.genouest.hadoopizer.formats.ChainValuesOutputFormat;
import org.genouest.hadoopizer.formats.FastqInputFormat;
import org.genouest.hadoopizer.mappers.GenericMapper;
import org.xml.sax.SAXException;

public class Hadoopizer {

    public static final Logger logger = Logger.getLogger("Hadoopizer"); // FIXME
                                                                        // make
                                                                        // hadoopizer
                                                                        // a
                                                                        // constant?

    private static final String VERSION = "1.0";

    /**
     * @param args
     *            command line arguments
     * @throws IOException
     * @throws SAXException
     * @throws ParserConfigurationException
     * @throws XPathExpressionException
     */
    public static void main(String[] args) throws IOException {

        Configuration jobConf = new Configuration();

        args = new GenericOptionsParser(jobConf, args).getRemainingArgs();

        if (args.length != 1) {
            System.err.println("Usage:");
            System.err.println("hadoop jar hadoopizer.jar config.xml"); // TODO
                                                                        // revoir
                                                                        // le
                                                                        // merdier
                                                                        // de
                                                                        // classpath/ant/sh
            System.exit(1);
        }

        if (args[0].equalsIgnoreCase("--version")
                || args[0].equalsIgnoreCase("-v"))
            version();

        if (args[0].equalsIgnoreCase("--help")
                || args[0].equalsIgnoreCase("-h"))
            help();

        // Load job config file
        File configFile = new File(args[0]);
        logger.info("reading config file: " + configFile.getAbsolutePath());

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

        logger.info("Will execute the following command: "
                + config.getRawCommand());

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

    private static Job prepareJob(JobConfig config, Configuration jobConf)
            throws IOException {

        Path inputPath = new Path(config.getSplittableInput().getUrl());

        jobConf.set("mapred.child.java.opts", "-Xmx1024m"); // FIXME make this
                                                            // configurable?
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
            Path hdfsBasePath = new Path("hdfs://192.168.2.20/data/tmp/"
                    + jobInput.getId() + Path.SEPARATOR); // FIXME ask the path
                                                          // from command line
                                                          // arg, and check that
                                                          // it doesn't already
                                                          // exist
            FileSystem fs = hdfsBasePath.getFileSystem(jobConf);

            // TODO distributed cache can also be used to distribute software

            if (jobInput.isAutoComplete()) {
                for (URI url : jobInput.getAllUrls()) {
                    // TODO compress the files maybe (archives are unarchived on
                    // the nodes)
                    Path localPath = new Path(url);
                    Path hdfsPath = new Path(hdfsBasePath.toString()
                            + Path.SEPARATOR + localPath.getName());// FIXME
                                                                    // make it
                                                                    // configurable/variable
                                                                    // somehow
                    logger.info("adding file '" + url
                            + "' to distributed cache (" + hdfsPath + ")");
                    fs.copyFromLocalFile(false, true, localPath, hdfsPath); // FIXME
                                                                            // make
                                                                            // it
                                                                            // work
                                                                            // for
                                                                            // all
                                                                            // protocols
                    // TODO avoid recopying if already existing

                    // Add a fragment to the uri: hadoop will automatically
                    // create a symlink in the work dir pointing to this file
                    // Don't add the fragment to hdfsPath because it would be
                    // encoded in a strange way
                    URI hdfsUri = URI.create(hdfsPath.toString()
                            + "#static_data__" + jobInput.getId() + "__"
                            + localPath.getName()); // FIXME check name
                                                    // collisions
                    DistributedCache.addCacheFile(hdfsUri, jobConf);
                }
            } else {
                Path localPath = new Path(jobInput.getUrl());
                Path hdfsPath = new Path(hdfsBasePath.toString()
                        + Path.SEPARATOR + localPath.getName()); // FIXME make
                                                                 // it
                                                                 // configurable/variable
                                                                 // somehow
                logger.info("adding file '" + jobInput.getUrl()
                        + "' to distributed cache (" + hdfsPath + ")");
                fs.copyFromLocalFile(false, true, localPath, hdfsPath); // FIXME
                                                                        // make
                                                                        // it
                                                                        // work
                                                                        // for
                                                                        // all
                                                                        // protocols
                // TODO avoid recopying if already existing

                // Add a fragment to the uri: hadoop will automatically create a
                // symlink in the work dir pointing to this file
                // Don't add the fragment to hdfsPath because it would be
                // encoded in a strange way
                URI hdfsUri = URI.create(hdfsPath.toString() + "#static_data__"
                        + jobInput.getId() + "__" + localPath.getName()); // FIXME
                                                                          // choose
                                                                          // a
                                                                          // better
                                                                          // fragment
                                                                          // (beware
                                                                          // of
                                                                          // name
                                                                          // collisions)
                DistributedCache.addCacheFile(hdfsUri, jobConf);
            }
        }

        DistributedCache.createSymlink(jobConf);

        // TODO create a hadoopizer.sh which will prepare the classpath instead
        // of using the manifest file

        // Create the job and its name
        Job job = new Job(jobConf, "Hadoopizer job");

        job.setJarByClass(Hadoopizer.class);

        // Set input path
        FastqInputFormat.setInputPaths(job, inputPath); // FIXME use the correct
                                                        // input format
                                                        // depending on
                                                        // config.xml

        // Set the Mapper class
        job.setMapperClass(GenericMapper.class);

        // Set the reducer class
        // job.setReducerClass(GenericReducer.class); // FIXME create a specific
        // one if some outputs types can be reduced before writing

        // Set the output key class
        job.setOutputKeyClass(Text.class); // FIXME use the correct type
                                           // depending on config.xml

        // Set the output value class
        job.setOutputValueClass(Text.class);// FIXME use the correct type
                                            // depending on config.xml

        // Set output path
        ChainValuesOutputFormat.setOutputPath(job, new Path(config
                .getJobOutput().getUrl())); // FIXME use a proper outputformat
                                            // class

        return job;
    }

    private static void help() {

        System.out.println("Usage:");
        System.out.println("hadoop jar hadoopizer.jar config.xml");
        System.exit(0);
    }

    private static void version() {

        System.out.println("Hadoopizer " + VERSION);
        System.exit(0);
    }

}
