package ass2;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class SumDecadeWritable implements Writable {
    private int sum;
    private int decade;

    public void write(DataOutput out) throws IOException {
        out.writeInt(sum);
        out.writeInt(decade);
    }

    public SumDecadeWritable(){}

    public SumDecadeWritable(int sum, int decade, int c1){
        this.sum = sum;
        this.decade = decade;
    }

    public void readFields(DataInput in) throws IOException {
        sum = in.readInt();
        decade = in.readInt();
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

    public static SumDecadeWritable read(DataInput in) throws IOException {
        SumDecadeWritable o = new SumDecadeWritable();
        o.readFields(in);
        return o;
    }

    public String toString(){
        return sum + "\t" + decade;
    }

}
