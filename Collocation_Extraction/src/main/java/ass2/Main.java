package ass2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.chain.Chain;

import java.io.IOException;
import java.util.Date;

public class Main {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        Configuration conf = new Configuration();
//First job
//        Job firstJob = getFirstJob(conf, "Input path", "output path");
//        Job secondJob = getSecondJob(conf, "Input path", "output path");
//        Job thirdJob = getThirdJob(conf, "Input path", "output path");
//        Job fourthJob = getFourthJob(conf, "Input path", "output path");
//        if (firstJob.waitForCompletion(true)) {
//            ControlledJob secondControlledJob = new ControlledJob(secondJob);
//            ControlledJob thirdControlledJob = new ControlledJob(thirdJob);
//            thirdControlledJob.addDependingJob(secondControlledJob);
//            ControlledJob fourthControlledJob = new ControlledJob(fourthJob);
//            fourthControlledJob.addDependingJob(thirdControlledJob);
//            JobControl jc = new JobControl("job_chaining");
//            jc.addJob(secondControlledJob);
//            jc.addJob(thirdControlledJob);
//            jc.addJob(fourthControlledJob);
//            jc.run();
//            code = jc.getFailedJobList().size() == 0 ? 0 : 1;


        Configuration C1CalcConf = new Configuration();
        String job1Name = "C1Calculator";
        String outputInput1 = job1Name + new Date().getTime();

        Configuration C2CalcConf = new Configuration();
        String job2Name = "C2Calculator";
        String outputInput2 = job2Name + new Date().getTime();

        Configuration npmiCalcConf = new Configuration();
        String job3Name = "NPMICalculator";
        String outputInput3 = job3Name + new Date().getTime();

        Configuration collocationConf = new Configuration();
        String job4Name = "Collocation";
        collocationConf.setDouble("minPmi", Double.parseDouble(args[1]));
        collocationConf.setDouble("relMinPmi", Double.parseDouble(args[2]));

        Job c1Calculator = createJob(C1CalcConf, job1Name, C1Calculator.class, C1Calculator.MapperClass.class, C1Calculator.ReducerClass.class,
                Decade2GramC1C2.class, StringIntWritable.class, Decade2GramC1C2.class, IntWritable.class,
                args[0], outputInput1, false);

        Job c2Calculator = createJob(C2CalcConf, job2Name, C2Calculator.class, C2Calculator.MapperClass.class, C2Calculator.ReducerClass.class,
                Decade2GramC1C2.class, StringIntIntWritable.class, Decade2GramC1C2.class, IntWritable.class,
                outputInput1 + "\\" + "part-r-00000", outputInput2, false);

        Job npmiCalculator = createJob(npmiCalcConf, job3Name, NPMICalculator.class, NPMICalculator.MapperClass.class, NPMICalculator.ReducerClass.class,
                Decade2GramC1C2.class, IntWritable.class, StringIntWritable.class, DoubleWritable.class,
                outputInput2 + "\\" + "part-r-00000", outputInput3, false);

        Job collocation = createJob(collocationConf, job4Name, Collocation.class, Collocation.MapperClass.class, Collocation.ReducerClass.class,
                IntDoubleStringWritable.class, Text.class, IntDoubleStringWritable.class, Text.class,
                outputInput3 + "\\" + "part-r-00000", job4Name + new Date().getTime(), true);

        if (c1Calculator.waitForCompletion(true)) {
            ControlledJob firstControlledJob = new ControlledJob(c1Calculator.getConfiguration());
            ControlledJob secondControlledJob = new ControlledJob(c2Calculator.getConfiguration());
            secondControlledJob.addDependingJob(firstControlledJob);
            ControlledJob thirdControlledJob = new ControlledJob(npmiCalculator.getConfiguration());
            thirdControlledJob.addDependingJob(secondControlledJob);
            ControlledJob fourthControlledJob = new ControlledJob(collocation.getConfiguration());
            fourthControlledJob.addDependingJob(thirdControlledJob);
            JobControl jc = new JobControl("job_chaining");
            jc.addJob(firstControlledJob);
            jc.addJob(secondControlledJob);
            jc.addJob(thirdControlledJob);
            jc.addJob(fourthControlledJob);
            jc.run();
            System.exit(jc.getFailedJobList().size() == 0 ? 0 : 1);
        }

    }


    public static Job createJob(Configuration conf, String jobName, Class jar, Class mapper, Class reducer, Class mapOutputKey, Class mapOutputValue,
                                Class outputKey, Class outputValue, String inputPath, String outputPath, boolean last) throws IOException, ClassNotFoundException, InterruptedException {
        Job job = Job.getInstance(conf, jobName);
        job.setJarByClass(jar);
        job.setMapperClass(mapper);
        job.setReducerClass(reducer);
        job.setMapOutputKeyClass(mapOutputKey);
        job.setMapOutputValueClass(mapOutputValue);
        job.setOutputKeyClass(outputKey);
        job.setOutputValueClass(outputValue);
//    job.setInputFormatClass(SequenceFileInputFormat.class);
        FileInputFormat.addInputPath(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));
        if (last) {
            System.exit(job.waitForCompletion(true) ? 0 : 1);
        } else {
            job.waitForCompletion(true);
        }
        return job;
    }
}
