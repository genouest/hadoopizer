package org.genouest.hadoopizer.mappers;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.genouest.hadoopizer.Hadoopizer;
import org.genouest.hadoopizer.JobConfig;

public class GenericMapper extends Mapper<LongWritable, Text, Text, Text> { 

    private JobConfig config;
    private File inputFile;
    private LongWritable firstKey;
    private LongWritable lastKey;
    private BufferedWriter writer;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {

        Configuration conf = context.getConfiguration();

        // Load hadoopizer job config
        String xmlConfig = conf.get("hadoopizer.job.config");
        config = new JobConfig();
        config.load(xmlConfig);

        // Download static files
        File workDir = new File(""); // The local work dir
        workDir = workDir.getAbsoluteFile();
        File[] workFiles = workDir.listFiles();
        Hadoopizer.logger.info("Looking for static input files in work dir: " + workDir.getAbsolutePath());
        Pattern p = Pattern.compile("static_data__(.*)__.*"); // FIXME bouh, hard coded
        Matcher m;
        String fileId;
        for (int i = 0; i < workFiles.length; i++) {
            m = p.matcher(workFiles[i].getName());

            if (m.matches()) {
                fileId = m.group(1);
                Hadoopizer.logger.info("Found the static input file '" + fileId + "' in the distributed cache: " + workFiles[i].toString());

                config.setStaticInputLocalPath(fileId, workFiles[i].toString());
            }
        }

        // Write data chunk to a temporary input file
        // This input file will be used in the command line launched in the cleanup step
        inputFile = createTempFile(new File(System.getProperty("java.io.tmpdir")), "input", ".chunk");
        writer = new BufferedWriter(new FileWriter(inputFile));
        Hadoopizer.logger.info("Writing input chunk to '" + inputFile.getAbsolutePath());

        config.getSplittableInput().setLocalPath(inputFile.getAbsolutePath());
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        //context.getCounter(GENERIC_COUNTER.INPUT_RECORDS).increment(1); // TODO this is useless -> find better use cases if needed

        // TODO take care of the chunk size (chromosome/read)

        if (firstKey == null)
            firstKey = key;
        lastKey = key;

        writer.write(value.toString(), 0, value.getLength());
        writer.newLine();
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {

        writer.flush();
        writer.close();
        
        // TODO maybe add a lock (optional?) to make sure only 1 command running at the same time on each node. This is done by eoulsan, but not necessarily useful.

        context.setStatus("Running command");

        // Preparing output file
        File outputFile = createTempFile(new File(System.getProperty("java.io.tmpdir")), "output", ".tmp"); // FIXME better tmp dir!
        config.getJobOutput().setLocalPath(outputFile.getAbsolutePath());
        Hadoopizer.logger.info("Saving temporary results in: " + outputFile);

        // Preparing the command line
        String command = config.getFinalCommand();
        Hadoopizer.logger.info("Running command: " + command);
        
        // java.lang.Process only works with 'simple' command lines (no redirections, ...)
        // Write the command line to a temp shell script
        File cmdFile = createTempFile(new File(System.getProperty("java.io.tmpdir")), "script", ".sh"); // FIXME better tmp dir!
        cmdFile.setExecutable(true);
        FileWriter fw = new FileWriter(cmdFile);
        BufferedWriter cmdWriter = new BufferedWriter(fw);
        cmdWriter.write("#!/bin/bash"); // FIXME dependency on bash
        cmdWriter.newLine();
        cmdWriter.write(command);
        cmdWriter.newLine();
        cmdWriter.write("cat "+cmdFile.getAbsolutePath()); // FIXME debug
        cmdWriter.flush();
        cmdWriter.close();

        // Running the command line
        Process p = Runtime.getRuntime().exec(cmdFile.getAbsolutePath());
        InputStream stdout = p.getInputStream();
        InputStream stderr = p.getErrorStream();
        
        // print stdout
        BufferedReader outReader = new BufferedReader (new InputStreamReader(stdout));
        String outLine;
        while ((outLine = outReader.readLine ()) != null) {
            Hadoopizer.logger.info("[stdout] " + outLine);
        }
        outReader.close();

        // print stderr
        outReader = new BufferedReader (new InputStreamReader(stderr));
        while ((outLine = outReader.readLine ()) != null) {
            Hadoopizer.logger.info("[stderr] " + outLine);
        }
        outReader.close();

        // Waiting for the command line completion
        int result = p.waitFor();
        if (result != 0) {
            throw new RuntimeException("Execution of command failed (returned " + result + ")");
        }

        // Process finished, get the output file content and add it to context
        context.setStatus("Preparing command output for reduce task");
        String outputContent = FileUtils.readFileToString(outputFile); // TODO handle binary results // FIXME OUPS !! out of memory sur le reducer!!
        // FIXME what about memory limit?

        Text key = new Text();
        Text value = new Text();
        key.set(firstKey + "-" + lastKey);
        value.set(outputContent);
        context.write(key, value);

        // Remove temporary output files
        // FIXME is it necessary?
        if (!outputFile.delete())
			Hadoopizer.logger.warning("Cannot delete output file: " + outputFile.getAbsolutePath());
    }


    // TODO externalize
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
