package ass1;

import org.apache.commons.io.FileUtils;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.ec2.model.DescribeImagesRequest;
import software.amazon.awssdk.services.ec2.model.DescribeImagesResponse;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.MessageAttributeValue;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class Tests {
    public static void main(String[] args) {

        final String outputName = "debug_cloud_local";
        File output = createOutputFileToCWD(outputName);

        ExecutorService pool = Executors.newFixedThreadPool(6);
        pool.execute(new Runnable() {
            @Override
            public void run() {
                String out = outputName;
            }
        });


        for (int i = 0; i < 3; ++i) {
            try {
                FileUtils.writeStringToFile(output, "numOfAnswers: " + i, "UTF-8");
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    public static File createOutputFileToCWD(String outputName) {
        try {
            File output = new File(outputName);

//            if(!output.setWritable(true,false)){
//                if(!output.canWrite()) {
//                    System.out.println("No permissions to set file as writeable");
//                    System.exit(1);
//                }
//            }
            if (output.createNewFile()) {
                System.out.println("File created: " + output.getName());
            } else {
                System.out.println("File already exists.");
            }
            return output;
        } catch (IOException e) {
            System.out.println("An error occurred.");
            e.printStackTrace();
        }

        return null;
    }
}
