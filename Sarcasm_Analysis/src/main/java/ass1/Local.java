package ass1;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import java.io.FileReader;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;

import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.ec2.model.*;
import software.amazon.awssdk.services.sqs.model.Message;


public class Local { //args[] == paths to input files
    public static void main(String[] args) throws IOException {
        if (args.length < 1) {
            System.out.println("no input files");
            System.exit(1);
        }

        Ec2Client ec2 = Ec2Client.create();
        if (!managerExists(ec2)) {
            CreateManager("manager1");
        }

        String bucketName = S3ObjectOperations.CreateBucket("inputFiles");
        String keys[] = S3ObjectOperations.PutObjects(args, bucketName);
        String SendQueueUrl = SendReceiveMessages.createSQS("localSendQueue");
        for (String key : keys) {
            SendReceiveMessages.send(SendQueueUrl, bucketName + ':' + key);
        }

        //wait for done messages
        String localRecieveQueueUrl = SendReceiveMessages.createSQS("loaclRecieveQueue");
        boolean stop = false;
        int counter = 0;
        List<Message> messages = null;
        while (!stop) { //busy wait until we get a done msg
            messages = SendReceiveMessages.receive(localRecieveQueueUrl);
            if (!messages.isEmpty())
                stop = true;
        }

        for (Message message : messages) {

            // current working directory = System.getProperty("user.dir")
            String[] split = message.body().split(":", 1);
            if (split.length != 2) {
                System.out.println("local didnt receive a done message with key and bucket or bucket/key string contanied a colon");
                System.exit(1);
            }
            ++counter;
            String key = split[0];
            String bucket = split[1];
            S3ObjectOperations.getObject(key, bucket, System.getProperty("user.dir") + "/output" + Integer.toString(counter));
        }
        
    }


    private static void CreateManager(String managerName) {
        String[] args = {managerName, "ami-0062dd78ec1ecd019", "manager"};
        CreateInstance.main(args);


    }

    public static boolean managerExists(Ec2Client ec2) {

        // Create a Filters to find a running manager
        Filter runningFilter = Filter.builder()
                .name("instance-state-name")
                .values("running")
                .build();

        Filter managerFilter = Filter.builder()
                .name("tag:job")
                .values("manager")
                .build();

        //Create a DescribeInstancesRequest
        DescribeInstancesRequest request = DescribeInstancesRequest.builder()
                .filters(managerFilter, runningFilter)
                .build();

        // Find the running manager instances
        DescribeInstancesResponse response = ec2.describeInstances(request);

        for (Reservation reservation : response.reservations()) {
            if (!reservation.instances().isEmpty()) {
                return true;
            }
        }

        return false;
    }

}
