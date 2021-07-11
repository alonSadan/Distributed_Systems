package ass2;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class SecondPairSumDecadeWritable implements Writable {
    private int sum;
    private int decade;
    private String secondPair;

    public void write(DataOutput out) throws IOException {
        out.writeInt(sum);
        out.writeInt(decade);
        out.writeBytes(secondPair);
    }

    public SecondPairSumDecadeWritable(){}

    public SecondPairSumDecadeWritable(int sum, int decade, String secondPair){
        this.sum = sum;
        this.decade = decade;
        this.secondPair = secondPair;
    }

    public void readFields(DataInput in) throws IOException {
        sum = in.readInt();
        decade = in.readInt();
        secondPair = in.readLine();
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

    public static SecondPairSumDecadeWritable read(DataInput in) throws IOException {
        SecondPairSumDecadeWritable o = new SecondPairSumDecadeWritable();
        o.readFields(in);
        return o;
    }

    public String toString(){
        return secondPair + "\t" + sum + "\t" + decade;
    }

    public void setSecondPair(String secondPair) {
        this.secondPair = secondPair;
    }
}
