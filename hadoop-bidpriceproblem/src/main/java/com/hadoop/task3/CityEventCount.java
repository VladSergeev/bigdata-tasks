package com.hadoop.task3;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class CityEventCount implements Writable {

    private LongWritable count;
    private LongWritable price;

    public CityEventCount() {
        this.count = new LongWritable();
        this.price = new LongWritable();
    }

    public CityEventCount(LongWritable count, LongWritable price) {
        this.count = count;
        this.price = price;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        count.write(dataOutput);
        price.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        count.readFields(dataInput);
        price.readFields(dataInput);
    }

    public void set( LongWritable count, LongWritable price) {
        this.count = count;
        this.price = price;
    }


    public LongWritable getCount() {
        return count;
    }

    public LongWritable getPrice() {
        return price;
    }

    @Override
    public String toString() {
        return "CityEventCount{" +
                "count=" + count +
                ", price=" + price +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        CityEventCount that = (CityEventCount) o;

        if (count != null ? !count.equals(that.count) : that.count != null) return false;
        return price != null ? price.equals(that.price) : that.price == null;
    }

    @Override
    public int hashCode() {
        int result = count != null ? count.hashCode() : 0;
        result = 31 * result + (price != null ? price.hashCode() : 0);
        return result;
    }
}
