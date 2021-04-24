package ass1;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import java.io.FileReader;
import java.io.IOException;
import java.util.*;

import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.ec2.model.*;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.MessageAttributeValue;

import static java.lang.Thread.sleep;


public class Local { //args[] == paths to input files
    public static void main(String[] args) throws IOException {
        if (args.length < 1) {
            System.out.println("no input files");
            System.exit(1);
        }

        String n = "";
        String[] inputs;
        if (args[args.length - 1].equals("terminate")) {
            n = args[args.length - 2];
            inputs = Arrays.copyOfRange(args, 0, args.length - 2);
        } else {
            n = args[args.length - 1];
            inputs = Arrays.copyOfRange(args, 0, args.length - 1);
        }

        Ec2Client ec2 = Ec2Client.create();
        if (!managerExists(ec2)) {
            CreateManager("manager");
        }

        SendInputsLocationsToManager(inputs, n);
        Message doneMessage = waitForDoneMessage();
        downloadSummeryFile(doneMessage);
    }

    public static void downloadSummeryFile(Message doneMessage) throws IOException {
        String bucket = SendReceiveMessages.extractAttribute(doneMessage, "bucket");
        String key = SendReceiveMessages.extractAttribute(doneMessage, "key");
        S3ObjectOperations.getObject(key, bucket, System.getProperty("user.dir") + "/output-" + String.valueOf(new Date().getTime()));
    }

    public static Message waitForDoneMessage() {
        String localRecieveQueueUrl = SendReceiveMessages.getQueueURLByName("loaclrecievequeue");
        boolean stop = false;
        Message doneMessage = null;
        while (!stop) { //busy wait until we get a done msg
            doneMessage = SendReceiveMessages.receive(localRecieveQueueUrl, "bucket", "key");
            if (doneMessage != null) {
                stop = true;
            }
            try {
                sleep(2000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        return doneMessage;
    }

    public static void SendInputsLocationsToManager(String[] inputs, String n) throws IOException {
        final Map<String, MessageAttributeValue> messageAttributes = new HashMap();
        String bucketName = S3ObjectOperations.CreateBucket("inputfiles");
        String keys[] = S3ObjectOperations.PutObjects(inputs, bucketName);
        // getQueueURLByName creates the queue if needed
        String SendQueueUrl = SendReceiveMessages.getQueueURLByName("localsendqueue");
        for (String key : keys) {
            MessageAttributeValue N = SendReceiveMessages.createStringAttributeValue(n);
            messageAttributes.put("n", N);

            MessageAttributeValue bucket = SendReceiveMessages.createStringAttributeValue(bucketName);
            messageAttributes.put("bucket", bucket);

            MessageAttributeValue KEY = SendReceiveMessages.createStringAttributeValue(key);
            messageAttributes.put("key", KEY);

            MessageAttributeValue localID = SendReceiveMessages.createStringAttributeValue(String.valueOf(new Date().getTime()));
            messageAttributes.put("localID", localID);

            SendReceiveMessages.send(SendQueueUrl, "", messageAttributes);
        }


    }

    private static void CreateManager(String managerName) {
        String script = "#!/bin/bash\n" +
                "cd AWS-files\n" +
                "java -cp 2-AWS-11.jar ass1/Manager\n";
        String[] args = {managerName, "ami-0062dd78ec1ecd019", script, "manager"};
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
