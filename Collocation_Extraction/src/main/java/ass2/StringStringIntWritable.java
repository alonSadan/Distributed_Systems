package ass2;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

public class StringStringIntWritable implements WritableComparable {
    private String twogram;
    private String secondPair;
    private int decade;

    public void write(DataOutput out) throws IOException {
        out.writeBytes(twogram);
        out.writeBytes(secondPair);
        out.writeInt(decade);
    }

    public StringStringIntWritable(){}

    public StringStringIntWritable(String twogram, String secondPair, int decade){
        // convert to utf-8
        byte[] bytes = twogram.getBytes(StandardCharsets.UTF_8);
        String utf8EncodedString = new String(bytes, StandardCharsets.UTF_8);
        twogram = utf8EncodedString;
        this.twogram = twogram;
        this.secondPair = secondPair;
        this.decade = decade;
    }

    public void readFields(DataInput in) throws IOException {
        twogram = in.readLine();
        secondPair = in.readLine();
        decade = in.readInt();
        // convert to utf-8
        byte[] bytes = twogram.getBytes(StandardCharsets.UTF_8);
        String utf8EncodedString = new String(bytes, StandardCharsets.UTF_8);
        twogram = utf8EncodedString;
    }

    public int getDecade() {
        return decade;
    }

    public String getTwogram() {
        return twogram;
    }

    public String getSecondPair() {
        return secondPair;
    }

    public void setDecade(int decade) {
        this.decade = decade;
    }

    public void setTwogram(String twogram) {
        this.twogram = twogram;
    }

    public void setSecondPair(String secondPair) {
        this.secondPair = secondPair;
    }

    public static StringStringIntWritable read(DataInput in) throws IOException {
        StringStringIntWritable si = new StringStringIntWritable();
        si.readFields(in);
        return si;
    }

    @Override
    public int compareTo(Object o) {
        int compare = ((StringStringIntWritable)o).getDecade() - this.getDecade();
        if ( compare == 0){
            return ((StringStringIntWritable)o).getTwogram().compareTo(this.getTwogram());
        }
        return compare;
    }

    public String toString(){
        return twogram + "\t" + secondPair + "\t" + decade;
    }
}
