package com.hadoop.task2;


import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class UserAgentInfo implements Writable {

    private LongWritable count;
    private LongWritable bytes;

    public UserAgentInfo() {
        this.count = new LongWritable();
        this.bytes = new LongWritable();
    }

    public UserAgentInfo(Long count, Long bytes) {
        this.count = new LongWritable(count);
        this.bytes = new LongWritable(bytes);
    }

    public LongWritable getCount() {
        return count;
    }

    public void setCount(LongWritable count) {
        this.count = count;
    }

    public LongWritable getBytes() {
        return bytes;
    }

    public void setBytes(LongWritable bytes) {
        this.bytes = bytes;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        count.write(dataOutput);
        bytes.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        count.readFields(dataInput);
        bytes.readFields(dataInput);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        UserAgentInfo that = (UserAgentInfo) o;

        if (count != null ? !count.equals(that.count) : that.count != null) {
            return false;
        }
        return bytes != null ? bytes.equals(that.bytes) : that.bytes == null;
    }

    @Override
    public int hashCode() {
        int result = count != null ? count.hashCode() : 0;
        result = 31 * result + (bytes != null ? bytes.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "UserAgentInfo{" +
                "count=" + count +
                ", bytes=" + bytes +
                '}';
    }
}
