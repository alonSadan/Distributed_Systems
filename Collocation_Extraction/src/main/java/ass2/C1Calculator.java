package ass2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.StringTokenizer;

public class C1Calculator {

    public static class MapperClass extends Mapper<LongWritable, Text, Decade2GramC1C2, StringIntWritable> {
        private Decade2GramC1C2 shlomo = new Decade2GramC1C2(3,"shlomo",1,2);
        private StringIntWritable si = new StringIntWritable("si",2);

//        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException,  InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString());
            String w1 = null;
            String w2 = null;
            int count = 0;
            int decade = 0;
            if(itr.countTokens() > 3){
                w1 = itr.nextToken();
                w2 = itr.nextToken();
                String strYear = itr.nextToken();
                decade = (Integer.parseInt(strYear.substring(0,3) + "0"));
                count = Integer.parseInt(itr.nextToken());
            }
            if (w1 == null || w2 == null){ // just to check
                return;
            }
            si.setNumber(count);
            si.setStr(w2);
            shlomo.setDecade(decade);
            shlomo.setWord(w1);
            context.write(shlomo, si);
        }
    }


    public static class ReducerClass extends Reducer<Decade2GramC1C2, StringIntWritable, Decade2GramC1C2, IntWritable> {
        private IntWritable num = new IntWritable(0);
//        @Override
        public void reduce(Decade2GramC1C2 key, Iterable<StringIntWritable> values, Context context) throws IOException,  InterruptedException {
            int sum = 0;
            int decade = key.getDecade();
            String word1 = key.getTwogram();
            ArrayList<StringIntWritable> l = new ArrayList();
            for (StringIntWritable value : values) {
                l.add(value);
                sum += value.getNumber();
            }
            for (StringIntWritable si : l){
                num.set(si.getNumber());
                context.write(new  Decade2GramC1C2(decade,word1 + " " + si.getStr(),
                        sum,0), num);
            }

        }
    }

//    public static class PartitionerClass extends Partitioner<Text, IntWritable> {
//      @Override
//      public int getPartition(Text key, IntWritable value, int numPartitions) {
//        return key.hashCode() % numPartitions;
//      }
//    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "c1_calculator");
        job.setJarByClass(C1Calculator.class);
        job.setMapperClass(MapperClass.class);
//    job.setPartitionerClass(PartitionerClass.class);
//    job.setCombinerClass(ReducerClass.class);
        job.setReducerClass(ReducerClass.class);
        job.setMapOutputKeyClass(Decade2GramC1C2.class);
        job.setMapOutputValueClass(StringIntWritable.class);
        job.setOutputKeyClass(Decade2GramC1C2.class);
        job.setOutputValueClass(IntWritable.class);
//    job.setNumReduceTasks(20);
//    job.setInputFormatClass(SequenceFileInputFormat.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}



