package ass2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.*;

public class C1Calculator {

    public static class MapperClass extends Mapper<LongWritable, Text, StringIntWritable, StringIntWritable> {
        private StringIntWritable shlomo = new StringIntWritable("s", 0);
        private StringIntWritable si = new StringIntWritable("si", 0);
        private final String stopWordsFileName = "stopWords" + new Date().getTime() + ".txt";
        fileContainsWord stopWordsFinder;
//        @Override
        @Override
        public void setup(Context context) throws IOException {
            //this will be executed once on each mapper before first map(..) call
            S3ObjectOperations.getObject("ass2jar", "stopWords.txt", stopWordsFileName);
            stopWordsFinder = new fileContainsWord(stopWordsFileName);
        }

        public void map(LongWritable key, Text value, Context context) throws IOException,  InterruptedException {
            String val = value.toString();
            String[] line = value.toString().split("\\s+");
            String w1 = null;
            String w2 = null;
            int count = 0;
            int decade = 0;
            if(line.length == 6){
                String strYear = line[2];
                try{
                    decade = (Integer.parseInt(strYear.substring(0,strYear.length() - 1) + "0"));
                } catch(Exception e){
                    System.err.println("the whole line is " + val);
                    System.err.println("the words that we get from value.toString().split(\"\\\\s+\"):");
                    for (int i = 0; i < line.length; i++){
                        System.err.println(line[i]);
                    }
                    //System.err.println("w1: " + line[0] + " w2: " + line[1] + " strYear: " + line[2] + " count: " + line[3]);
                    e.printStackTrace();
                    System.exit(1);
                }
                w1 = line[0];
                w2 = line[1];
                count = Integer.parseInt(line[3]);
            }
            else{
                return;
            }
            if (stopWordsFinder.contains(w1) || stopWordsFinder.contains(w2)){
                return;
            }
            si.setStr(w2);
            si.setNumber(count);
            shlomo.setNumber(decade);
            shlomo.setStr(w1);
            shlomo.setFlag((char) 0); // we cant go over values in the reducer twice. so we send them 2 times
            context.write(shlomo, si);
            shlomo.setFlag((char) 1); // set flag
            context.write(shlomo, si);
        }

//        public void map(LongWritable key, Text value, Context context) throws IOException,  InterruptedException {
//            String val = value.toString();
//            String[] line = value.toString().split("\\s+");
//            String w1 = null;
//            String w2 = null;
//            int count = 0;
//            int decade = 0;
//            if(line.length > 3){
//                String strYear = line[2];
//                try{
//                    decade = (Integer.parseInt(strYear.substring(0,3) + "0"));
//                } catch(Exception e){
//                    System.err.println("the whole line is " + val);
//                    System.err.println("the words that we get from value.toString().split(\"\\\\s+\"):");
//                    for (int i = 0; i < line.length; i++){
//                        System.err.println(line[i]);
//                    }
//                    //System.err.println("w1: " + line[0] + " w2: " + line[1] + " strYear: " + line[2] + " count: " + line[3]);
//                    e.printStackTrace();
//                    System.exit(1);
//                }
//                w1 = line[0];
//                w2 = line[1];
//                count = Integer.parseInt(line[3]);
//            }
//            if (stopWordsFinder.contains(w1) || stopWordsFinder.contains(w2)){
//                return;
//            }
//
//            si.setStr(w2);
//            si.setNumber(count);
//            shlomo.setDecade(decade);
//            shlomo.setWord(w1);
//            shlomo.setFlag(0); // we cant go over values in the reducer twice. so we send them 2 times
//            context.write(shlomo, si);
//            shlomo.setFlag(1); // set flag
//            context.write(shlomo, si);
//        }

    }

    public static class C1Combiner extends Reducer<StringIntWritable, StringIntWritable, StringIntWritable, StringIntWritable> {
        public void reduce(StringIntWritable key, Iterable<StringIntWritable> values, Mapper.Context context) throws IOException, InterruptedException {
            int sum = 0;
            char flag = key.getFlag();
            if (flag == (char)1) { // flag
                for (StringIntWritable value : values) {
                    sum += value.getNumber();
                }
                context.write(key, new StringIntWritable("*", sum));
            } else {
                for (StringIntWritable value : values) {
                    context.write(key, value);
                }
            }
        }
    }



    // alon, yotam(8), flag
    // alon, yotam(8)
    // alon, gabi(11), flasg
    // alon, gabi(11)

    //combiner:
    // if(flag) write(alon, *(19))
    // if(!flag) write(alon, yotam(8) or (alon,gabi(11))



//    combining all the strings is bad, since the reducer will need to create an array with o(n) words

    public static class ReducerClass extends Reducer<StringIntWritable, StringIntWritable, StringIntIntWritable, IntWritable> {
        private IntWritable num = new IntWritable(0);
        private int sum = 0;

        //        @Override
        public void reduce(StringIntWritable key, Iterable<StringIntWritable> values, Context context) throws IOException, InterruptedException {
            int decade = key.getNumber();
            char flag = key.getFlag();
            String word1 = key.getStr();
            if (flag == (char)1) { // flag
                for (StringIntWritable value : values) {
                    sum += value.getNumber();
                }
            } else {
                for (StringIntWritable value : values) {
                    num.set(value.getNumber());
                    context.write(new StringIntIntWritable(word1 + " " + value.getStr(), decade,
                            sum), num);
                }
                sum = 0; // initilize sum again
            }
        }
    }

    public static class PartitionerClass extends Partitioner<StringIntWritable, StringIntWritable> {
      @Override
      public int getPartition(StringIntWritable key, StringIntWritable value, int numPartitions) {
        return ((key.getNumber() % 100) / 10) % numPartitions; // partition by decade
      }
    }

   public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "c1_calculator");
        job.setJarByClass(C1Calculator.class);
        job.setMapperClass(MapperClass.class);
        job.setPartitionerClass(PartitionerClass.class);
//        job.setCombinerClass(C1Combiner.class);
        job.setReducerClass(ReducerClass.class);
        job.setMapOutputKeyClass(StringIntWritable.class);
        job.setMapOutputValueClass(StringIntWritable.class);
        job.setOutputKeyClass(StringIntIntWritable.class);
        job.setOutputValueClass(IntWritable.class);

        job.setInputFormatClass(SequenceFileInputFormat.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);

   }

}



