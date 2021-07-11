package ass2;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

public class StringIntIntWritable implements WritableComparable {
    private String str;
    private int number1;
    private int number2;

    public void write(DataOutput out) throws IOException {
        out.writeUTF(str);
        out.writeInt(number1);
        out.writeInt(number2);
    }

    public StringIntIntWritable(){}

    public StringIntIntWritable(String str, int number1, int number2){
        // convert to utf-8
        byte[] bytes = str.getBytes(StandardCharsets.UTF_8);
        String utf8EncodedString = new String(bytes, StandardCharsets.UTF_8);
        str = utf8EncodedString;
        this.str = str;
        this.number1 = number1;
        this.number2 = number2;
    }

    public void readFields(DataInput in) throws IOException {
        str = in.readUTF();
        number1 = in.readInt();
        number2 = in.readInt();
        // convert to utf-8
        byte[] bytes = str.getBytes(StandardCharsets.UTF_8);
        String utf8EncodedString = new String(bytes, StandardCharsets.UTF_8);
        str = utf8EncodedString;
    }

    public int getNumber1() {
        return number1;
    }

    public int getNumber2() {
        return number2;
    }

    public String getStr() {
        return str;
    }


    public void setNumber1(int number1) {
        this.number1 = number1;
    }

    public void setNumber2(int number2) {
        this.number2 = number2;
    }

    public void setStr(String str) {
        this.str = str;
    }


    public static StringIntIntWritable read(DataInput in) throws IOException {
        StringIntIntWritable si = new StringIntIntWritable();
        si.readFields(in);
        return si;
    }

    @Override
    public int compareTo(Object o) {
        int compare = ((StringIntIntWritable)o).getNumber1() - this.getNumber1();
//        if ( compare == 0){
//            return ((StringIntWritable)o).getStr().compareTo(this.getStr());
//        }
        return compare;
    }

    public String toString(){
        return str + "\t" + number1 + "\t" + number2;
    }
}
