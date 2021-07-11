package ass2;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.StringTokenizer;

public class Collocation {
    public static class MapperClass extends Mapper<LongWritable, Text, IntDoubleStringWritable, Text> {
        private IntDoubleStringWritable decadeNpmiTwogram = new IntDoubleStringWritable();

        //        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException,  InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString());
            String w1 = null;
            String w2 = null;
            int decade = 0;
            double npmi = 0;
            if(itr.countTokens() > 3){
                w1 = itr.nextToken();
                w2 = itr.nextToken();
                decade = Integer.parseInt(itr.nextToken());
                npmi = Double.parseDouble(itr.nextToken());
            }
            if (w1 == null || w2 == null){ // just to check
                return;
            }
            decadeNpmiTwogram.setDuble(npmi);
            decadeNpmiTwogram.setInteger(decade);
            decadeNpmiTwogram.setStr(w1 + " " + w2);
            context.write(decadeNpmiTwogram, new Text("ghgh"));
        }
    }


    public static class ReducerClass extends Reducer<IntDoubleStringWritable, Text, IntDoubleStringWritable, Text> {
        private double minPmi = 0.5;
        private double relMinPmi = 0.2;
        private double decadeNPMI = 0.0;
        private int run = 0;

        //        @Override
        public void reduce(IntDoubleStringWritable key, Iterable<Text> values, Context context) throws IOException,  InterruptedException {
            if (key.getStr().equals("* *")) {
                decadeNPMI = key.getDouble();
            } else {
                double npmi = key.getDouble();
                // C1Calculator discards the stop words
                if ( (npmi > minPmi) || ( (npmi / decadeNPMI) > relMinPmi) ) {
                    context.write(key, new Text(""));
                }
            }
        }

    }

//    public static class PartitionerClass extends Partitioner<Text, IntWritable> {
//      @Override
//      public int getPartition(Text key, IntWritable value, int numPartitions) {
//        return key.hashCode() % numPartitions;
//      }
//    }

//    public static void main(String[] args) throws Exception {
//        Configuration conf = new Configuration();
//        Job job = Job.getInstance(conf, "Collacation");
//        job.setJarByClass(Collocation.class);
//        job.setMapperClass(Collocation.MapperClass.class);
////    job.setPartitionerClass(PartitionerClass.class);
////    job.setCombinerClass(ReducerClass.class);
//        job.setReducerClass(Collocation.ReducerClass.class);
//
//        job.setMapOutputKeyClass(IntDoubleStringWritable.class);
//
//        job.setMapOutputValueClass(Text.class);
//
//        job.setOutputKeyClass(IntDoubleStringWritable.class);
//
//        job.setOutputValueClass(Text.class);
////    job.setNumReduceTasks(20);
////    job.setInputFormatClass(SequenceFileInputFormat.class);
//        FileInputFormat.addInputPath(job, new Path(args[0]));
//        FileOutputFormat.setOutputPath(job, new Path(args[1]));
//        System.exit(job.waitForCompletion(true) ? 0 : 1);
//    }
}
