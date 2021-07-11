package ass2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class Main {
    public void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        Configuration conf = new Configuration();
//First job
        Job firstJob = getFirstJob(conf, "Input path", "output path");
        Job secondJob = getSecondJob(conf, "Input path", "output path");
        Job thirdJob = getThirdJob(conf, "Input path", "output path");
        Job fourthJob = getFourthJob(conf, "Input path", "output path");
        if (firstJob.waitForCompletion(true)) {
            ControlledJob secondControlledJob = new ControlledJob(secondJob);
            ControlledJob thirdControlledJob = new ControlledJob(thirdJob);
            thirdControlledJob.addDependingJob(secondControlledJob);
            ControlledJob fourthControlledJob = new ControlledJob(fourthJob);
            fourthControlledJob.addDependingJob(thirdControlledJob);
            JobControl jc = new JobControl("job_chaining");
            jc.addJob(secondControlledJob);
            jc.addJob(thirdControlledJob);
            jc.addJob(fourthControlledJob);
            jc.run();
            code = jc.getFailedJobList().size() == 0 ? 0 : 1;



//        Configuration conf1 = new Configuration();
//        Configuration conf2 = new Configuration();
//        Configuration conf3 = new Configuration();
//        Configuration conf4 = new Configuration();
//        createJob(conf1,"C1Calculator", C1Calculator.class, C1Calculator.MapperClass.class, C1Calculator.ReducerClass.class,
//                Decade2GramC1C2.class, StringIntWritable.class, Decade2GramC1C2.class, IntWritable.class,
//                args[0], "C1Calculator",false);
//
//        createJob(conf2,"C2Calculator", C2Calculator.class, C2Calculator.MapperClass.class, C2Calculator.ReducerClass.class,
//                Decade2GramC1C2.class, StringIntIntWritable.class, Decade2GramC1C2.class, IntWritable.class,
//                "C1Calculator" + "\\" + "part-r-00000", "C2Calculator",false);
//
//        createJob(conf3,"NPMICalculator", NPMICalculator.class, NPMICalculator.MapperClass.class, NPMICalculator.ReducerClass.class,
//                Decade2GramC1C2.class, IntWritable.class, StringIntWritable.class, DoubleWritable.class,
//                "C2Calculator" + "\\" + "part-r-00000", "NPMICalculator",false);
//
//        createJob(conf4,"Collocation", Collocation.class, Collocation.MapperClass.class, Collocation.ReducerClass.class,
//                IntDoubleStringWritable.class, Text.class, IntDoubleStringWritable.class, Text.class,
//                "NPMICalculator" + "\\" + "part-r-00000", "Collocation", true);
    }



    public void createJob(Configuration conf, String jobName, Class jar, Class mapper, Class reducer, Class mapOutputKey, Class mapOutputValue,
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
        if (last){
            System.exit(job.waitForCompletion(true) ? 0 : 1);
        }
        else{
            job.waitForCompletion(true);
        }
    }
}
