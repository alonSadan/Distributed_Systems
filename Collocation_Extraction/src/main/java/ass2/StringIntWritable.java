package ass2;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

public class StringIntWritable implements WritableComparable {
    private String str;
    private int number;
    private char flag;

    public void write(DataOutput out) throws IOException {
        out.writeUTF(str);
        out.writeInt(number);
        out.writeChar(flag);
    }

    public StringIntWritable(){}

    public StringIntWritable(String str, int number){
        // convert to utf-8
        byte[] bytes = str.getBytes(StandardCharsets.UTF_8);
        String utf8EncodedString = new String(bytes, StandardCharsets.UTF_8);
        str = utf8EncodedString;
        this.str = str;
        this.number = number;
    }

    public void readFields(DataInput in) throws IOException {
        str = in.readUTF();
        number = in.readInt();
        flag = in.readChar();
        // convert to utf-8
        byte[] bytes = str.getBytes(StandardCharsets.UTF_8);
        String utf8EncodedString = new String(bytes, StandardCharsets.UTF_8);
        str = utf8EncodedString;
    }

    public int getNumber() {
        return number;
    }

    public String getStr() {
        return str;
    }

    public char getFlag(){ return flag;}

    public void setFlag(char flag){ this.flag = flag;}

    public void setNumber(int number) {
        this.number = number;
    }

    public void setStr(String str) {
        this.str = str;
    }


    public static StringIntWritable read(DataInput in) throws IOException {
        StringIntWritable si = new StringIntWritable();
        si.readFields(in);
        return si;
    }

    @Override
    public int compareTo(Object o) {
        int decadeCompare = ((StringIntWritable)o).getNumber() - this.getNumber();
        if ( decadeCompare == 0){
            int strCompare =  ((StringIntWritable)o).getStr().compareTo(this.getStr());
            if (strCompare == 0){
                return ((StringIntWritable)o).getFlag() - this.getFlag();
            }
            else {
                return strCompare;
            }
        }
        return decadeCompare;
    }

    public String toString(){
        return str + "\t" + number;
    }
}
