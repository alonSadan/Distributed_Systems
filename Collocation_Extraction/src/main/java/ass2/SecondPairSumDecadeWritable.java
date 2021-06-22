package ass2;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class TwogramSumDecadeWritable implements Writable {
    private int sum;
    private int decade;
    private String twogram;

    public void write(DataOutput out) throws IOException {
        out.writeInt(sum);
        out.writeInt(decade);
        out.writeBytes(twogram);
    }

    public TwogramSumDecadeWritable(){}

    public TwogramSumDecadeWritable(int sum, int decade, String twogram){
        this.sum = sum;
        this.decade = decade;
        this.twogram = twogram;
    }

    public void readFields(DataInput in) throws IOException {
        sum = in.readInt();
        decade = in.readInt();
        twogram = in.readLine();
    }

    public int getSum() {
        return sum;
    }

    public int getDecade() {
        return decade;
    }

    public void setDecade(int decade) {
        this.decade = decade;
    }

    public void setSum(int sum) {
        this.sum = sum;
    }

    public static TwogramSumDecadeWritable read(DataInput in) throws IOException {
        TwogramSumDecadeWritable o = new TwogramSumDecadeWritable();
        o.readFields(in);
        return o;
    }

    public String toString(){
        return twogram + "\t" + sum + "\t" + decade;
    }

    public void setTwogram(String twogram) {
        this.twogram = twogram;
    }
}
