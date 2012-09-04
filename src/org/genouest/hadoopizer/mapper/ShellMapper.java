package org.genouest.hadoopizer.mapper;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ObjectWritable;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.genouest.hadoopizer.Hadoopizer;
import org.genouest.hadoopizer.JobConfig;
import org.genouest.hadoopizer.JobInputFile;
import org.genouest.hadoopizer.JobOutput;
import org.genouest.hadoopizer.SplitableJobInput;
import org.genouest.hadoopizer.input.HadoopizerInputFormat;
import org.genouest.hadoopizer.input.HadoopizerRecordReader;
import org.genouest.hadoopizer.io.ObjectWritableComparable;
import org.genouest.hadoopizer.output.HadoopizerOutputFormat;

public class ShellMapper extends Mapper<ObjectWritableComparable, ObjectWritable, ObjectWritableComparable, ObjectWritable> { 

    private JobConfig config;
    private ArrayList<RecordWriter<ObjectWritableComparable, ObjectWritable>> writers;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {

        Configuration conf = context.getConfiguration();

        // Load hadoopizer conf config
        String xmlConfig = conf.get("hadoopizer.job.config");
        config = new JobConfig();
        config.load(xmlConfig);

        // Download static files
        File workDir = new File(""); // The local work dir
        workDir = workDir.getAbsoluteFile();
        File[] workFiles = workDir.listFiles();
        Hadoopizer.logger.info("Looking for static input files in work dir: " + workDir.getAbsolutePath());
        Pattern p = Pattern.compile(conf.get("hadoopizer.static.data.link.prefix") + "(.*)__.*");
        Matcher m;
        String fileId;
        for (int i = 0; i < workFiles.length; i++) {
            m = p.matcher(workFiles[i].getName());

            if (m.matches()) {
                fileId = m.group(1);
                Hadoopizer.logger.info("Found the static input file '" + fileId + "' in the distributed cache: " + workFiles[i].toString());

                config.setStaticInputLocalPath(fileId, workFiles[i].toString()); // FIXME maybe some refactoring here
            }
        }

        // Write data chunk to a temporary input file
        // This input file will be used in the command line launched in the cleanup step
        SplitableJobInput splitable = (SplitableJobInput) config.getSplitableInput();
        int nb = 0;
        for (JobInputFile file : splitable.getFiles()) {
            
            HadoopizerOutputFormat outf = file.getFileOutputFormat();
            File inputFile = Hadoopizer.createTempFile(new File(System.getProperty("java.io.tmpdir")), "input_" + nb, "."+outf.getExtension());
            
            // We want to add the header from input file to each chunk file
            Path headerFile = new Path(context.getConfiguration().get("hadoopizer.temp.input.header.file") + "_" + nb); // FIXME this is a mess, check it works with multiple
            outf.setHeaderTempFile(headerFile);
            
            RecordWriter<ObjectWritableComparable, ObjectWritable> writer = (RecordWriter<ObjectWritableComparable, ObjectWritable>) outf.getRecordWriter(context, new Path("file:"+inputFile.getAbsolutePath()), null);
            writers.add(writer);
            
            Hadoopizer.logger.info("Writing input chunk to '" + inputFile.getAbsolutePath() + "' with OutputFormat class '" + writer.getClass().getCanonicalName() + "'");
    
            file.setLocalPath(inputFile.getAbsolutePath());
            nb++;
        }
    }


    @Override
    protected void map(ObjectWritableComparable key, ObjectWritable value, Context context) throws IOException, InterruptedException {
        // TODO support multiple inputs : receive a tuplewritable
        for (RecordWriter<ObjectWritableComparable, ObjectWritable> writer : writers) {
            writer.write(key, value);
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {

        for (RecordWriter<ObjectWritableComparable, ObjectWritable> writer : writers) {
            writer.close(context);
        }
        
        context.setStatus("Running command");

        // Preparing output files
        HashSet<JobOutput> outs = config.getJobOutputs();
        for (JobOutput out : outs) {
            File outputFile = Hadoopizer.createTempFile(new File(System.getProperty("java.io.tmpdir")), "output_" + out.getId(), ".tmp");
            out.setLocalPath(outputFile.getAbsolutePath());
            Hadoopizer.logger.info("Saving temporary results in: " + outputFile);
        }

        // Preparing the command line
        String command = config.getFinalCommand();
        Hadoopizer.logger.info("Running command: " + command);
        
        // java.lang.Process only works with 'simple' command lines (no redirections, ...)
        // Write the command line to a temp shell script
        File cmdFile = Hadoopizer.createTempFile(new File(System.getProperty("java.io.tmpdir")), "script", ".sh");
        cmdFile.setExecutable(true);
        FileWriter fw = new FileWriter(cmdFile);
        BufferedWriter cmdWriter = new BufferedWriter(fw);
        cmdWriter.write(context.getConfiguration().get("hadoopizer.shell.interpreter"));
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

        // Process finished, get the output files content and add it to context
        for (JobOutput out : outs) {
            context.setStatus("Parsing command output with " + out.getReducerId() + " parser for " + out.getId() + " output");
        
            HadoopizerInputFormat inf = out.getFileInputFormat();
            
            // We want to add the header in the final output file
            Path headerFile = new Path(context.getConfiguration().get("hadoopizer.temp.output.header.file") + "_" + out.getId());
            inf.setHeaderTempFile(headerFile);
            
            File outFile = new File(out.getLocalPath());
            InputSplit split = new FileSplit(new Path(outFile.toURI()), 0, outFile.length(), null);
            HadoopizerRecordReader reader = (HadoopizerRecordReader) inf.createRecordReader(split, context);
            reader.initialize(split, context);
            while (reader.nextKeyValue()) {
                context.write(reader.getCurrentKey(out.getId()), reader.getCurrentValue());
            }
            reader.close();
    
            // Remove temporary output files
            if (!outFile.delete())
    			Hadoopizer.logger.warning("Cannot delete output file: " + outFile.getAbsolutePath());
        }
        
        context.setStatus("Finished");
    }
}
