package org.genouest.hadoopizer.mappers;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.RecordReader;
import org.genouest.hadoopizer.Hadoopizer;
import org.genouest.hadoopizer.JobConfig;

public class GenericMapper extends Mapper<Text, Text, Text, Text> { 

    private JobConfig config;
    private File inputFile;
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
        inputFile = Hadoopizer.createTempFile(new File(System.getProperty("java.io.tmpdir")), "input", ".chunk");
        writer = new BufferedWriter(new FileWriter(inputFile));
        Hadoopizer.logger.info("Writing input chunk to '" + inputFile.getAbsolutePath());

        config.getSplittableInput().setLocalPath(inputFile.getAbsolutePath());
    }

    @Override
    protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
        
        // TODO make this step dependant of the input format!!
        writer.write("@" + key.toString());
        writer.newLine();
        writer.write(value.toString());
        writer.newLine();
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {

        writer.flush();
        writer.close();
        
        // TODO maybe add a lock (optional?) to make sure only 1 command running at the same time on each node. This is done by eoulsan, but not necessarily useful.

        context.setStatus("Running command");

        // Preparing output file
        File outputFile = Hadoopizer.createTempFile(new File(System.getProperty("java.io.tmpdir")), "output", ".tmp");
        config.getJobOutput().setLocalPath(outputFile.getAbsolutePath());
        Hadoopizer.logger.info("Saving temporary results in: " + outputFile);

        // Preparing the command line
        String command = config.getFinalCommand();
        Hadoopizer.logger.info("Running command: " + command);
        
        // java.lang.Process only works with 'simple' command lines (no redirections, ...)
        // Write the command line to a temp shell script
        File cmdFile = Hadoopizer.createTempFile(new File(System.getProperty("java.io.tmpdir")), "script", ".sh");
        cmdFile.setExecutable(true);
        FileWriter fw = new FileWriter(cmdFile);
        BufferedWriter cmdWriter = new BufferedWriter(fw);
        cmdWriter.write("#!/bin/bash"); // FIXME dependency on bash
        cmdWriter.newLine();
        cmdWriter.write(command);
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
        context.setStatus("Parsing command output with " + config.getJobOutput().getReducer() + " parser");
        FileInputFormat<?, ?> inf = config.getJobOutput().getFileInputFormat();
        InputSplit split = new FileSplit(new Path(outputFile.toURI()), 0, outputFile.length(), null);
        RecordReader<Text, Text> reader = (RecordReader<Text, Text>) inf.createRecordReader(split, context); // FIXME problem with generic class
        reader.initialize(split, context);
        while (reader.nextKeyValue()) {
            context.write(reader.getCurrentKey(), reader.getCurrentValue());
        }
        reader.close();

        // Remove temporary output files
        if (!outputFile.delete())
			Hadoopizer.logger.warning("Cannot delete output file: " + outputFile.getAbsolutePath());
        
        context.setStatus("Finished");
    }
}
