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
import java.util.Date;
import java.util.StringTokenizer;

public class C2Calculator {
    public static class MapperClass extends Mapper<LongWritable, Text, Decade2GramC1C2, StringIntIntWritable> {
        private Decade2GramC1C2 shlomo = new Decade2GramC1C2(3, "shlomo", 0, 0, 0);
        private StringIntIntWritable sii = new StringIntIntWritable("si", 2, 3);

        //        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString());
            String w1 = null;
            String w2 = null;
            int count = 0;
            int decade = 0;
            int c1 = 0;
            int c2 = 0;
            if (itr.countTokens() > 3) {
                decade = Integer.parseInt(itr.nextToken());
                w1 = itr.nextToken();
                w2 = itr.nextToken();
                c1 = Integer.parseInt(itr.nextToken());
                c2 = Integer.parseInt(itr.nextToken()); // only to get the tokenizer take the right token
                count = Integer.parseInt(itr.nextToken());
            }
            if (w1 == null || w2 == null) { // just to check
                return;
            }
            sii.setStr(w1);      // this is the main point of C2Calculator
            sii.setNumber1(c1);
            sii.setNumber2(count);
            shlomo.setDecade(decade);
            shlomo.setWord(w2); // this is the main point of C2Calculator
            shlomo.setFlag(0);
            context.write(shlomo, sii);
            shlomo.setFlag(1);
            context.write(shlomo, sii);
        }
    }

    public static class C2Combiner extends Reducer<Decade2GramC1C2, StringIntIntWritable, Decade2GramC1C2, StringIntIntWritable> {
        public void reduce(Decade2GramC1C2 key, Iterable<StringIntIntWritable> values, Mapper.Context context) throws IOException, InterruptedException {
            int sum = 0;
            int flag = key.getFlag();
            if (flag == 1) { // flag
                for (StringIntIntWritable value : values) {
                    sum += value.getNumber2();
                }

                context.write(key, new StringIntIntWritable("*", -1, sum));
            } else {
                for (StringIntIntWritable value : values) {
                    context.write(key, value);
                }
            }
        }
    }

    public static class ReducerClass extends Reducer<Decade2GramC1C2, StringIntIntWritable, Decade2GramC1C2, IntWritable> {
        private IntWritable count = new IntWritable(0);
        private int sum = 0;

        //        @Override
        public void reduce(Decade2GramC1C2 key, Iterable<StringIntIntWritable> values, Context context) throws IOException, InterruptedException {
            int decade = key.getDecade();
            String word2 = key.getTwogram();
            if (key.getFlag() == 1) {
                for (StringIntIntWritable value : values) {
                    sum += value.getNumber2();
                }
            } else {
                for (StringIntIntWritable value : values) {
                    count.set(value.getNumber2());
                    context.write(new Decade2GramC1C2(decade, value.getStr() + " " + word2,
                            value.getNumber1(), sum, 0), count);
                }
                sum = 0; // initilize sum again
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
        Job job = Job.getInstance(conf, "c2_calculator");
        job.setJarByClass(C2Calculator.class);
        job.setMapperClass(C2Calculator.MapperClass.class);
//    job.setPartitionerClass(PartitionerClass.class);
        job.setCombinerClass(C2Combiner.class);
        job.setReducerClass(C2Calculator.ReducerClass.class);
        job.setMapOutputKeyClass(Decade2GramC1C2.class);
        job.setMapOutputValueClass(StringIntIntWritable.class);
        job.setOutputKeyClass(Decade2GramC1C2.class);
        job.setOutputValueClass(IntWritable.class);
//    job.setNumReduceTasks(20);
//    job.setInputFormatClass(SequenceFileInputFormat.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
