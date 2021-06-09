package ass2;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

public class StringIntWritable implements WritableComparable {
    private int decade;
    private String twogram;

    public void write(DataOutput out) throws IOException {
        out.writeInt(decade);
        out.writeBytes(twogram);
    }

    public StringIntWritable(){}

    public StringIntWritable(String twogram, int decade){
        // convert to utf-8
        byte[] bytes = twogram.getBytes(StandardCharsets.UTF_8);
        String utf8EncodedString = new String(bytes, StandardCharsets.UTF_8);
        twogram = utf8EncodedString;
        this.twogram = twogram;
        this.decade = decade;
    }

    public void readFields(DataInput in) throws IOException {
        decade = in.readInt();
        twogram = in.readLine();
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

    public void setDecade(int decade) {
        this.decade = decade;
    }

    public void setTwogram(String twogram) {
        this.twogram = twogram;
    }

    public static StringIntWritable read(DataInput in) throws IOException {
        StringIntWritable si = new StringIntWritable();
        si.readFields(in);
        return si;
    }

    @Override
    public int compareTo(Object o) {
        int compare = ((StringIntWritable)o).getDecade() - this.getDecade();
        if ( compare == 0){
            return ((StringIntWritable)o).getTwogram().compareTo(this.getTwogram());
        }
        return compare;
    }

    public String toString(){
        return twogram + "\t" + decade;
    }
}
