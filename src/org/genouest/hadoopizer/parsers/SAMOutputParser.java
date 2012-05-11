package org.genouest.hadoopizer.parsers;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;

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

        while ((line = samReader.readLine()) != null) {

            String trimmedLine = line.trim();
            if ("".equals(trimmedLine) || trimmedLine.startsWith("@")) { // FIXME can we do something with headers? yes, merge them
                Hadoopizer.logger.info("SAM comment: " + trimmedLine); // FIXME debug
                continue;
            }

            int tabPos = line.indexOf('\t');

            if (tabPos != -1) {

                outKey.set(line.substring(0, tabPos)); // TODO can it be a problem if multiple lines with same id (ie read mapping at several locations)?
                outValue.set(line.substring(tabPos + 1));

                entriesParsed++;

                context.write(outKey, outValue);
                context.getCounter("SAMContent", "mapped.reads").increment(1); // TODO use GENERIC_COUNTER
            }

        }

        samReader.close();

        Hadoopizer.logger.info(entriesParsed + " entries parsed in SAM output file");
    }

    @Override
    public String getId() {
        
        return "sam";
    }
}
