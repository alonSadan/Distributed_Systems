package ass2;

import com.amazonaws.AmazonClientException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.ec2.model.IamInstanceProfileSpecification;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduce;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduceClientBuilder;
import com.amazonaws.services.elasticmapreduce.model.*;

import com.amazonaws.services.elasticmapreduce.util.StepFactory;

import com.amazonaws.AmazonClientException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.EnvironmentVariableCredentialsProvider;
import com.amazonaws.auth.PropertiesCredentials;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.ec2.model.InstanceType;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduce;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduceClient;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduceClientBuilder;
import com.amazonaws.services.elasticmapreduce.model.*;
import software.amazon.awssdk.auth.credentials.InstanceProfileCredentialsProvider;

import java.io.File;
import java.io.IOException;

public class Runner {

    public static void main(String[] args) throws IOException {

        AWSCredentials credentials_profile = null;
        try {
            credentials_profile = new ProfileCredentialsProvider("default").getCredentials();
        } catch (Exception e) {
            e.printStackTrace();
        }

        AmazonElasticMapReduce emr = AmazonElasticMapReduceClientBuilder.standard()
                .withCredentials(new AWSStaticCredentialsProvider(credentials_profile))
                .withRegion(Regions.US_EAST_1)
                .build();

       // AWSCredentials credentials = new PropertiesCredentials(new File("C:\\Users\\alons\\.aws\\credentials"));

        // AmazonElasticMapReduce mapReduce = AmazonElasticMapReduceClient.builder().withRegion("us-east-1") withCredentials(new com.amazonaws.auth.InstanceProfileCredentialsProvider()).build();
        // AmazonElasticMapReduce mapReduce = AmazonElasticMapReduceClient.builder().build();
        // "s3://datasets.elasticmapreduce/ngrams/books/20090715/heb-all/2gram/data"    s3://ass2jar/heb-2-gram-example.txt
        HadoopJarStepConfig step1 = createJarStep("s3://ass2jar/collocation_extraction-1.0-SNAPSHOT-jar-with-dependencies.jar", "ass2.C1Calculator", "s3://datasets.elasticmapreduce/ngrams/books/20090715/heb-all/2gram/data", "s3://ass2jar/output1/");
        HadoopJarStepConfig step2 = createJarStep("s3://ass2jar/collocation_extraction-1.0-SNAPSHOT-jar-with-dependencies.jar", "ass2.C2Calculator", "s3://ass2jar/output1/part-r-00000", "s3://ass2jar/output2/");
        HadoopJarStepConfig step3 = createJarStep("s3://ass2jar/collocation_extraction-1.0-SNAPSHOT-jar-with-dependencies.jar", "ass2.NPMICalculator", "s3://ass2jar/output2/part-r-00000", "s3://ass2jar/output3/");
        HadoopJarStepConfig step4 = createJarStep("s3://ass2jar/collocation_extraction-1.0-SNAPSHOT-jar-with-dependencies.jar", "ass2.Collocation", "s3://ass2jar/output3/part-r-00000", "s3://ass2jar/output4/", args[0], args[1]);

        StepConfig stepConfig1 = createStepConfig("c1Calculator", step1, "TERMINATE_JOB_FLOW");
        StepConfig stepConfig2 = createStepConfig("c2Calculator", step2, "TERMINATE_JOB_FLOW");
        StepConfig stepConfig3 = createStepConfig("npmiCalculator", step3, "TERMINATE_JOB_FLOW");
        StepConfig stepConfig4 = createStepConfig("collocations", step4, "TERMINATE_JOB_FLOW");

        JobFlowInstancesConfig instances = new JobFlowInstancesConfig()
                .withInstanceCount(2)
                .withMasterInstanceType(InstanceType.M5Xlarge.toString())
                .withSlaveInstanceType(InstanceType.M5Xlarge.toString())
                .withHadoopVersion("3.2.1").withEc2KeyName("ass2")
                .withKeepJobFlowAliveWhenNoSteps(false)
                .withPlacement(new PlacementType("us-east-1a"));
        RunJobFlowRequest runFlowRequest = new RunJobFlowRequest()
                .withName("jobname")
                .withInstances(instances)
                .withServiceRole("EMR_DefaultRole")
                .withJobFlowRole("EMR_EC2_DefaultRole")
                .withReleaseLabel("emr-6.3.0")
                .withSteps(stepConfig1,stepConfig2, stepConfig3, stepConfig4)
                .withLogUri("s3://asss2logs");
        RunJobFlowResult runJobFlowResult = emr.runJobFlow(runFlowRequest);
        String jobFlowId = runJobFlowResult.getJobFlowId();
        System.out.println("Ran job flow with id: " + jobFlowId);
    }

    private static HadoopJarStepConfig createJarStep(String jar, String mainClass, String... args) {
        HadoopJarStepConfig hadoopJarStep = new HadoopJarStepConfig()
                .withJar(jar) // This should be a full map reduce application.
                .withMainClass(mainClass)
                .withArgs(args);
        return hadoopJarStep;
    }

    private static StepConfig createStepConfig(String name, HadoopJarStepConfig step, String actionOnFailure) {
        StepConfig stepConfig = new StepConfig()
                .withName(name)
                .withHadoopJarStep(step)
                .withActionOnFailure(actionOnFailure);
        return stepConfig;
    }
}
