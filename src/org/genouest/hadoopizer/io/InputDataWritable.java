package org.genouest.hadoopizer.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.ObjectWritable;
import org.apache.hadoop.io.Writable;

public class InputDataWritable implements Writable {

    private int inputId;
    private ObjectWritable data;
    
    public InputDataWritable() {
        
    }
    
    public InputDataWritable(int inputId, ObjectWritable data) {
        set(inputId, data);
    }

    public void set(int inputId, ObjectWritable data) {
        this.inputId = inputId;
        this.data = data;
    }
    
    public int getInputId() {
        return inputId;
    }
    
    public ObjectWritable getData() {
        return data;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(inputId);
        data.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        inputId = in.readInt();
        data = new ObjectWritable();
        data.setConf(new Configuration());
        data.readFields(in);
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof InputDataWritable))
            return false;
        InputDataWritable other = (InputDataWritable)o;
        return (this.inputId == other.inputId) && (this.data == other.data);
    }

    @Override
    public int hashCode() {
      return inputId + data.hashCode();
    }

    @Override
    public String toString() {
      return Integer.toString(inputId) + ", " + data.toString();
    }
}
