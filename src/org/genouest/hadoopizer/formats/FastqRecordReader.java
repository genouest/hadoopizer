package org.genouest.hadoopizer.formats;

import java.io.IOException;
import java.nio.charset.Charset;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

public class FastqRecordReader extends RecordReader<LongWritable, Text> {

    private static final Charset CHARSET = Charset.forName("UTF-8");

    private long start;
    private long end;
    private boolean stillInChunk = true;
    private long splitStart;

    private FSDataInputStream fsin;
    private DataOutputBuffer buffer = new DataOutputBuffer();

    private byte[] endTag = null;

    private LongWritable recordKey = new LongWritable();
    private Text recordValue = new Text();


    @Override
    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {

        endTag = "\n@".getBytes("UTF-8"); // FIXME there can be some @ in the quality line

        Configuration job = context.getConfiguration();

        FileSplit fileSplit = (FileSplit) split;

        Path path = fileSplit.getPath();
        FileSystem fs = path.getFileSystem(job);

        fsin = fs.open(path);
        start = fileSplit.getStart();
        splitStart = start;
        end = fileSplit.getStart() + fileSplit.getLength();
        fsin.seek(start);

        if (start != 0) { // Beginning of the whole file
            readUntilMatch(endTag, false);
        }
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {

        if (!stillInChunk) // End of split/file reached
            return false;

        long startPos = fsin.getPos();

        boolean status = readUntilMatch(endTag, true);

        String data;

        // If not the start of the file, add missing '@' (removed with regex matching)
        data = new String(buffer.getData(), 0, buffer.getLength(), CHARSET);
        if (startPos != splitStart)
            data = "@" + data;
        // If not the start of the file, remove the '@' (added with regex matching)
        data = data.substring(0, data.length() - endTag.length);

        recordKey.set(fsin.getPos());
        recordValue.set(data); // Dump fastq record

        buffer.reset();

        if (!status) // End of split/file reached
            stillInChunk = false;

        return true;
    }

    @Override
    public LongWritable getCurrentKey() throws IOException, InterruptedException {

        return recordKey;
    }

    @Override
    public Text getCurrentValue() throws IOException, InterruptedException {

        return recordValue;
    }

    @Override
    public void close() throws IOException {

        fsin.close();
    }

    @Override
    public float getProgress() throws IOException {

        if (start == end) {
            return (float)0;
        } else {
            return Math.min((float)1.0, (fsin.getPos() - start) / (float)(end - start));
        } 
    }

    /**
     * Read the split and fill the buffer with one record
     * 
     * @param match the characters expected at the beginning of the next record (ie where we should stop)
     * @param withinBlock True if we are in a record, false otherwise
     * @return false if there is no more data to read (end of file, or end of split reached), true otherwise
     * @throws IOException
     */
    private boolean readUntilMatch(byte[] match, boolean withinBlock) throws IOException {
        int i = 0;
        while (true) {
            int b = fsin.read();
            
            if (b == -1) // End of the file
                return false;
            
            if (withinBlock) // In a record, fill the buffer
                buffer.write(b);
            
            if (b == match[i]) { // one char of the match param recognized
                i++;
                if (i >= match.length) { // The full match param has been found, we have reached a new record
                    return fsin.getPos() < end; // Did we reached the end of the split?
                }
            } else
                i = 0;
        }
    }
}
