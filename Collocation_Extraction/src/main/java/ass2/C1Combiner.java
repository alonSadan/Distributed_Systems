//package ass2;
//
//import org.apache.hadoop.conf.Configuration;
//import org.apache.hadoop.fs.Path;
//import org.apache.hadoop.io.IntWritable;
//import org.apache.hadoop.io.LongWritable;
//import org.apache.hadoop.io.Text;
//import org.apache.hadoop.mapreduce.Job;
//import org.apache.hadoop.mapreduce.Mapper;
//import org.apache.hadoop.mapreduce.Reducer;
//import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
//import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
//import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
//
//import java.io.File;
//import java.io.FileNotFoundException;
//import java.io.IOException;
//import java.util.*;
//
//import java.io.IOException;
//
//public static class C1Combiner {
//    public void reduce(Decade2GramC1C2 key, Iterable<StringIntWritable> values, Context context) throws IOException,  InterruptedException{
//        int sum = 0;
//        int decade = key.getDecade();
//        int flag = key.getFlag();
//        String word1 = key.getTwogram();
//        if (flag == 1){ // flag
//            for (StringIntWritable value : values) {
//                sum += value.getNumber();
//            }
//        }
//
//    }
//}
