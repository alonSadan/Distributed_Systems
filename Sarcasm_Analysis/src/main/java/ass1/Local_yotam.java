package ass1;

import java.io.IOException;
import java.util.List;

import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.ec2.model.*;
import software.amazon.awssdk.services.sqs.model.Message;


public class Local_yotam { //args[] == paths to input files
    public static void main(String[] args) throws IOException {
        if (args.length < 1) {
            System.out.println("no input files");
            System.exit(1);
        }
        String n = "";
        if(args[args.length - 1].equals("terminate"))
            n = args[args.length -2];
        else
            n = args[args.length-1];

        Ec2Client ec2 = Ec2Client.create();
        if (!instanceExists(ec2, "manager")) { // we might need manager activation if we already have a stopped manager
            CreateManager("manager1");
        }

        // send Manager.java to ec2/S3
        //AmazonCodeDeployClient

        String bucketName = S3ObjectOperations_yotam.CreateBucket("inputfiles");
        String[] keys = S3ObjectOperations_yotam.PutObjects(args, bucketName);
        String SendQueueUrl = SendReceiveMessages_yotam.createSQS("localsendqueue");
        SendReceiveMessages_yotam.send(SendQueueUrl, n);
        for (String key : keys) {
            SendReceiveMessages_yotam.send(SendQueueUrl, bucketName + ':' + key);
        }

        //wait for done messages
        String localRecieveQueueUrl = SendReceiveMessages_yotam.createSQS("loaclrecievequeue");
        boolean stop = false;
        int counter = 0;
        List<Message> messages = null;
        messages = SendReceiveMessages_yotam.receive(localRecieveQueueUrl); // for testing, dont busy-wait
//        while (!stop) { //busy wait until we get a done msg
//            messages = SendReceiveMessages.receive(localRecieveQueueUrl);
//            if (!messages.isEmpty())
//                stop = true;
//        }

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
            S3ObjectOperations_yotam.getObject(key, bucket, System.getProperty("user.dir") + "/output" + Integer.toString(counter));
        }
        
    }


    private static void CreateManager(String managerName) {
        String script = "cd AWS-files\njava -jar Worker.jar Worker";
        String[] args = {managerName, "ami-0062dd78ec1ecd019",script, "manager"};
        CreateInstance_yotam.main(args);


    }

    public static boolean instanceExists(Ec2Client ec2, String job) {

        // Create a Filters to find a running manager
        Filter runningFilter = Filter.builder()
                .name("instance-state-name")
                .values("running")
                .build();

        Filter managerFilter = Filter.builder()
                .name("tag:job")
                .values(job)
                .build();

        //Create a DescribeInstancesRequest
        DescribeInstancesRequest request = DescribeInstancesRequest.builder()
                .filters(managerFilter, runningFilter)
                .build();

        // Find the running job instances
        DescribeInstancesResponse response = ec2.describeInstances(request);

        for (Reservation reservation : response.reservations()) {
            if (!reservation.instances().isEmpty()) {
                return true;
            }
        }

        return false;
    }



}
