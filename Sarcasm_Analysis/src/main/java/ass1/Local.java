package ass1;


import java.io.File;
import java.io.IOException;
import java.util.*;

import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.ec2.model.*;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.MessageAttributeValue;

import static java.lang.Thread.sleep;


public class Local { //args[] == paths to input files

    private static String ID  = String.valueOf(new Date().getTime());

    public static void main(String[] args) throws IOException {
        if (args.length < 1) {
            System.out.println("no input files");
            System.exit(1);
        }
        boolean shouldTerminate = false;
        String n = "";
        String[] inputs;
        if (args[args.length - 1].equals("terminate")) {
            shouldTerminate = true;
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
        if(shouldTerminate){
            terminateManager();
        }
    }

    public static void terminateManager(){
        String SendQueueUrl = SendReceiveMessages.getQueueURLByName("localsendqueue");
        final Map<String, MessageAttributeValue> messageAttributes = new HashMap();
        MessageAttributeValue terminate = SendReceiveMessages.createStringAttributeValue("true");
        messageAttributes.put("terminate", terminate);
        SendReceiveMessages.send(SendQueueUrl, "terminate", messageAttributes);
    }

    public static void downloadSummeryFile(Message doneMessage) throws IOException {
        System.out.println("downloading html");
        String bucket = SendReceiveMessages.extractAttribute(doneMessage, "bucket");
        String key = SendReceiveMessages.extractAttribute(doneMessage, "key");
        S3ObjectOperations.getObject(bucket, key, System.getProperty("user.dir") + File.separator + "output-" + String.valueOf(new Date().getTime()));
    }

    public static Message waitForDoneMessage() {
        String localRecieveQueueUrl = SendReceiveMessages.getQueueURLByName("loaclrecievequeue");
        boolean stop = false;
        Message doneMessage = null;
        while (!stop) { //busy wait until we get a done msg
            doneMessage = SendReceiveMessages.receive(localRecieveQueueUrl, "bucket", "key", "localID");
            String localID = SendReceiveMessages.extractAttribute(doneMessage,"localID");
            if (doneMessage != null && localID != null && localID.equals(ID)) {
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
        String messageBody = "";
        for (String key : keys) {
            messageBody += (bucketName + ":" + key + ":" );
        }
        messageBody = messageBody.substring(0, messageBody.length() - 1);
        MessageAttributeValue N = SendReceiveMessages.createStringAttributeValue(n);
        messageAttributes.put("n", N);

        MessageAttributeValue localID = SendReceiveMessages.createStringAttributeValue(ID);
        messageAttributes.put("localID", localID);
        SendReceiveMessages.send(SendQueueUrl, messageBody, messageAttributes);
    }
//
    private static void CreateManager(String managerName) {
        String script = "#! /bin/bash\n" +
                "java -jar /home/ec2-user/manager-1.0-jar-with-dependencies.jar\n";
        String[] args = {managerName, "ami-0eed7b9090ae0b59f", script, "manager"};
        CreateInstance.main(args);
    }

    public static boolean managerExists(Ec2Client ec2) {

        // Create a Filters to find a manager
        Filter managerFilter = Filter.builder()
                .name("tag:job")
                .values("manager")
                .build();

        //Create a DescribeInstancesRequest
        DescribeInstancesRequest request = DescribeInstancesRequest.builder()
                .filters(managerFilter)
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
