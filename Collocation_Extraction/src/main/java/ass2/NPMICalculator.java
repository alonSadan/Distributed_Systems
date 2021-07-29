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

public class NPMICalculator {
    public static class MapperClass extends Mapper<LongWritable, Text, Decade2GramC1C2, IntWritable> {
        private Decade2GramC1C2 shlomo = new Decade2GramC1C2(3,"shlomo",0,0,0);
        private IntWritable count = new IntWritable(0);
        private IntWritable one = new IntWritable(1);

        //        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException,  InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString());
            String w1 = null;
            String w2 = null;
            int decade = 0;
            int c1 = 0;
            int c2 = 0;
            if(itr.countTokens() > 3){
                decade = Integer.parseInt(itr.nextToken());
                w1 = itr.nextToken();
                w2 = itr.nextToken();
                c1 = Integer.parseInt(itr.nextToken());
                c2 = Integer.parseInt(itr.nextToken());
                count.set(Integer.parseInt(itr.nextToken()));
            }
            if (w1 == null || w2 == null){ // just to check
                return;
            }
            shlomo.setC1(c1);
            shlomo.setC2(c2);
            shlomo.setDecade(decade);
            shlomo.setWord(w1 + " " + w2);
            context.write(shlomo, count);
            shlomo.setWord("*N*");
            context.write(shlomo,one);
        }
    }


    public static class NPMICombiner extends Reducer<Decade2GramC1C2, IntWritable, Decade2GramC1C2, IntWritable> {
        public void reduce(Decade2GramC1C2 key, Iterable<IntWritable> values, Mapper.Context context) throws IOException, InterruptedException {
            int sum = 0;
            if(key.getTwogram().equals("*N*")) {
                for (IntWritable value : values) {
                    sum += value.get();
                }

                context.write(key, new IntWritable(sum));
            }
        }
    }

    public static class ReducerClass extends Reducer<Decade2GramC1C2, IntWritable, StringIntWritable, DoubleWritable> {
        private IntWritable count = new IntWritable(0);
        private int N = 0;
        private double decade_NPMI = 0.0;
        private int curr_decade = 0;
        private Decade2GramC1C2 last_input_key = new Decade2GramC1C2();
        private IntWritable last_input_value = new IntWritable(0);

        //        @Override
        public void reduce(Decade2GramC1C2 key, Iterable<IntWritable> values, Context context) throws IOException,  InterruptedException {
            int decade = key.getDecade();
            String twogram = key.getTwogram();
            if(key.getTwogram().equals("*N*")) {
                for (IntWritable value : values) {
                    N += value.get();
                }
            }
            else{
                if (curr_decade != key.getDecade())  {  			// start of new deacade
                    if (curr_decade !=0){       // NOT start of program
                        context.write(new StringIntWritable("* *",curr_decade),new DoubleWritable(decade_NPMI));
                    }
                    decade_NPMI = 0;
                    curr_decade = key.getDecade();
                }

                double decadeCount = 0;
                for (IntWritable value : values) {
                    decadeCount += value.get();
                }
                double P_w1w2 = decadeCount/N;
                int c1 = key.getC1();
                int c2 = key.getC2();
                double PMI_w1w2 = Math.log(decadeCount) + Math.log(N) - Math.log(c1)-Math.log(c2);

                double NPMI_w1w2 = PMI_w1w2 / ( (-1) * Math.log(P_w1w2) );

                context.write(new StringIntWritable(twogram,decade),new DoubleWritable(NPMI_w1w2));
                decade_NPMI += NPMI_w1w2;
                last_input_key = key.getCopy();
                last_input_value.set((int)decadeCount); // decadeCount is a sum of integers
            }

        }

        public void cleanup(Context context) throws IOException, InterruptedException {
            context.write(new StringIntWritable("* *",last_input_key.getDecade()),new DoubleWritable(last_input_value.get())); // <<last_input.key.decade, **>decade_NPMI>
            decade_NPMI = 0;
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
        Job job = Job.getInstance(conf, "NPMI_calculator");
        job.setJarByClass(NPMICalculator.class);
        job.setMapperClass(NPMICalculator.MapperClass.class);
//    job.setPartitionerClass(PartitionerClass.class);
        job.setCombinerClass(NPMICombiner.class);
        job.setReducerClass(NPMICalculator.ReducerClass.class);

        job.setMapOutputKeyClass(Decade2GramC1C2.class);

        job.setMapOutputValueClass(IntWritable.class);

        job.setOutputKeyClass(StringIntWritable.class);

        job.setOutputValueClass(DoubleWritable.class);
//    job.setNumReduceTasks(20);
//    job.setInputFormatClass(SequenceFileInputFormat.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
