/**
 * Big Data Project - MapReduce
 *
 * Muhamad Aldy B.
 * Source: https://hadoop.apache.org/docs/current/hadoop-mapreduce-client/hadoop-mapreduce-client-core/MapReduceTutorial.html
 */

/**
 * Imported classes
 */
package ass2;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordCountInternet {

    /**
     * Mapper class
     *
     * This class has function to mapping the input file into some key with each of them
     * will contains something that in common. It's like an array with key is the name while
     * the data with same type of key is the value.
     *
     * This mapper class will mapping the words inside the input file with every word contains
     * resemble as the key. For every word found in the file will be counted and the word itself
     * represent as the key of value.
     */
    public static class TextMapper extends Mapper<Object, Text, Text, IntWritable> {

        // initialize the field variable
        private final static IntWritable myMapper = new IntWritable(1);
        private Text myWordMap = new Text();

        /**
         * This map method is use for mapping the words into word key
         *
         * @param key                       Object variable for the keys based on the word being founded
         * @param value                     The word that will represent as the key if not exists
         * @param context                   Context of the java class, contains information to passing class
         * @throws IOException              Input-Output java exception, throws when input/output is cannot be processed
         * @throws InterruptedException     Interrupt java exception, throws when interrupt is occurred during task
         */
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            // create new object
            StringTokenizer itr = new StringTokenizer(value.toString());

            // the mapper loop as the word founded is already exists or not as the key for reducer
            // if exists then put it into the same key object
            // if not then write as new key
            while (itr.hasMoreTokens()) {
                myWordMap.set(itr.nextToken());
                context.write(myWordMap, myMapper);
            }
        }
    }

    /**
     * Reducer class
     *
     * This class has function to reducer job from mapped data which Mapper class has worked on. In this class,
     * job will be done by reducing the data, or value, with the same key and produce the new data named Reduced
     * Data, which means reducing the data from the big input data. It's more like reducing the mapped data with
     * the key value which has been determined and producing as the users or developers wanted. For example to know
     * how many data inside the big file with predefined key.
     *
     * This reducer class will get the data from the mapper class and counting how many words has been found with
     * the same key object. The output will counting the words found inside the files and produce the output file
     * with "key" then "how many been found".
     */
    public static class CountReducer extends Reducer<Text,IntWritable,Text,IntWritable> {

        // initialized the field variable
        private IntWritable myReducer = new IntWritable();

        /**
         * This reduce method is use for reducing the mapped data into same key with how many value being found
         *
         * @param myKey                     Object variable for the keys based on the word being founded
         * @param values                    The counter variable for counting how many words with the same key
         * @param context                   Context of the java class, contains information to passing class
         * @throws IOException              Input-Output java exception, throws when input/output is cannot be processed
         * @throws InterruptedException     Interrupt java exception, throws when interrupt is occurred during task
         */
        public void reduce(Text myKey, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

            // set sum to zero, initial value
            int sum = 0;

            // for every Object founded in the mapped data with same key, counting up
            for (IntWritable val : values) {
                sum += val.get();
            }

            // set the reducer with count value
            myReducer.set(sum);

            // write into the text will be produced as an output
            context.write(myKey, myReducer);
        }
    }

    /**
     * Main Java class
     *
     * This is the main class to be run inside this project. It includes how to start the Map and Reduce Job,
     * using the Hadoop Mapper class method to mapping with our Mapper class, using the Hadoop Reducer class
     * method to reducing with our Reducer class, getting the files and produce the output files, and finish
     * the both job and also determine how to exit the program.
     *
     * @param args          Main arguments, it's basic of main java class
     * @throws Exception    Exception for any error found, throws when any exception been found
     */
    public static void main(String[] args) throws Exception {

        // getting new configuration object
        Configuration myConf = new Configuration();

        // determine job with the configuration and job name
        Job job = Job.getInstance(myConf, "WordCount");

        // use the mapper class into the job
        job.setJarByClass(WordCountInternet.class);
        job.setMapperClass(TextMapper.class);
        job.setCombinerClass(CountReducer.class);

        // use the reducer class into the job
        job.setReducerClass(CountReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // called the input path for file
        FileInputFormat.addInputPath(job, new Path(args[0]));

        // defined the output path for produced file
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // determine how to exit (0 = success, 1 = failure, there is exception occurred)
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
