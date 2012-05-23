package org.genouest.hadoopizer.parsers;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.genouest.hadoopizer.Hadoopizer;

public class SAMOutputParser implements OutputParser {

    @Override
    public void parse(File samFile, Mapper<?, ?, Text, Text>.Context context) throws IOException, InterruptedException { // FIXME  will we need another type some day?

        String line;

        Text outKey = new Text();
        Text outValue = new Text();

        FileInputStream samIs = new FileInputStream(samFile);
        BufferedReader samReader = new BufferedReader(new InputStreamReader(samIs));

        int entriesParsed = 0;
        
        Configuration conf = context.getConfiguration();
        String headerFileName = conf.get("hadoopizer.temp.header.file");
        Path headerFile = new Path(headerFileName);
        FileSystem fs = headerFile.getFileSystem(conf);
        FSDataOutputStream headerOut = null;
        if (!fs.exists(headerFile)) {
            try {
                headerOut = fs.create(headerFile, false);
            } catch (IOException e) {
                headerOut = null;
            }
        }
        
        while ((line = samReader.readLine()) != null) {

            String trimmedLine = line.trim();
            if ("".equals(trimmedLine)) {
                continue;
            }
            else if (trimmedLine.startsWith("@")) {
                if (headerOut != null) {
                    // This is a SAM header line: write it in a temp file to prepend to the output file
                    headerOut.write(trimmedLine.getBytes());
                    headerOut.write("\n".getBytes());
                }
                continue;
            }

            int tabPos = line.indexOf('\t');

            if (tabPos != -1) {

                outKey.set(line.substring(0, tabPos));
                outValue.set(line.substring(tabPos + 1));

                entriesParsed++;

                context.write(outKey, outValue);
                context.getCounter("SAMContent", "mapped.reads").increment(1); // TODO use GENERIC_COUNTER
            }

        }

        samReader.close();
        if (headerOut != null) {
            headerOut.close();
        }

        Hadoopizer.logger.info(entriesParsed + " entries parsed in SAM output file");
    }

    @Override
    public String getId() {
        
        return "sam";
    }
}
