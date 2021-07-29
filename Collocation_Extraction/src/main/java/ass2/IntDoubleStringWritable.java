package ass2;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

public class IntDoubleStringWritable implements WritableComparable {
    private int integer;
    private double duble;
    private String str;

    public void write(DataOutput out) throws IOException {
        out.writeInt(integer);
        out.writeDouble(duble);
        out.writeUTF(str);
    }

    public IntDoubleStringWritable(){}

    public IntDoubleStringWritable(int integer, double duble, String str){
        // convert to utf-8
        byte[] bytes = str.getBytes(StandardCharsets.UTF_8);
        String utf8EncodedString = new String(bytes, StandardCharsets.UTF_8);
        str = utf8EncodedString;
        this.str = str;
        this.integer = integer;
        this.duble = duble;
    }

    public void readFields(DataInput in) throws IOException {
        integer = in.readInt();
        duble = in.readDouble();
        str = in.readUTF();
        // convert to utf-8
        byte[] bytes = str.getBytes(StandardCharsets.UTF_8);
        String utf8EncodedString = new String(bytes, StandardCharsets.UTF_8);
        str = utf8EncodedString;
    }

    public int getInt() {
        return integer;
    }

    public double getDouble() {
        return duble;
    }

    public String getStr() {
        return str;
    }

    public void setInteger(int integer) {
        this.integer = integer;
    }

    public void setDuble(double duble) {
        this.duble = duble;
    }

    public void setStr(String str) {
        this.str = str;
    }

    public static IntDoubleStringWritable read(DataInput in) throws IOException {
        IntDoubleStringWritable si = new IntDoubleStringWritable();
        si.readFields(in);
        return si;
    }

    @Override
    public int compareTo(Object o) {
        int compare = ((IntDoubleStringWritable)o).getInt() - this.getInt(); //decade
        if ( compare == 0){
            if ( this.getStr().equals("* *") ){
                return 1;
            }
            if ( ((IntDoubleStringWritable)o).getStr().equals("* *") ){
                return -1;
            }
            double npmiCompare = ((IntDoubleStringWritable)o).getDouble() - this.getDouble();
            if (npmiCompare < 0){
                return -1;
            }
            else
                return 1;
        }
        return compare;
    }

    public String toString(){
        return str + "\t" + integer + "\t" + duble;
    }
}
