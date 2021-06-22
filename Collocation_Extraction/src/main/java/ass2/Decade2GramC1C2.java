package ass2;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class Decade2GramC1C2 implements WritableComparable {
    private int decade;
    private String twogram;
    private int c1;
    private int c2;

    public Decade2GramC1C2() {}

    public Decade2GramC1C2(int decade, String twogram, int c1, int c2) {
        this.decade = decade;
        this.twogram = twogram;
        this.c1 = c1;
        this.c2 = c2;
    }

    public int getDecade() {
        return decade;
    }

    public String getTwogram() {
        return twogram;
    }

    public int getC1() {
        return c1;
    }

    public int getC2() {
        return c2;
    }

    public void setDecade(int decade) {
        this.decade = decade;
    }

    public void setWord(String twogram) {
        this.twogram = twogram;
    }

    public void setC1(int c1) {
        this.c1 = c1;
    }

    public void setC2(int c2) {
        this.c2 = c2;
    }

    @Override
    public int compareTo(Object o) {
        int compare = ((Decade2GramC1C2)o).getDecade() - this.getDecade();
        if ( compare == 0){
            if ( this.getTwogram().equals("*N*") ){
                return 1;
            }
            if ( ((Decade2GramC1C2)o).getTwogram().equals("*N*") ){
                return -1;
            }

            return ((Decade2GramC1C2)o).getTwogram().compareTo(this.getTwogram());
        }
        return compare;
    }

    public void write(DataOutput out) throws IOException {
        out.writeInt(decade);
        out.writeUTF(twogram);
        out.writeInt(c1);
        out.writeInt(c2);
    }

    public void readFields(DataInput in) throws IOException {
        decade = in.readInt();
        twogram = in.readUTF();
        c1 = in.readInt();
        c2 = in.readInt();
    }

    public static Decade2GramC1C2 read(DataInput in) throws IOException {
        Decade2GramC1C2 shlomi = new Decade2GramC1C2();
        shlomi.readFields(in);
        return shlomi;
    }

    public String toString(){ return decade + "\t" + twogram + "\t" + c1 + "\t" + c2; }

}
