package ass1;

import jdk.internal.net.http.common.Pair;
import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.ec2.model.*;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.MessageAttributeValue;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

import static java.lang.Thread.sleep;
import static java.util.Collections.emptyList;


/*
how to check the manager?
1. create a queue called "localsendqueue", that has a message with n, bucketname and key
2. put a textfile in the bucket
3. send a message to localsendqueue that has an attribute "terminate"
** dont busy-wait in terminate
 */

// - "mvn package" creates a jar file
// - running "java -cp 2-files-test-1.0-SNAPSHOT.jar Hello" prints hello
public class Manager {
    public static void main(String[] args) throws IOException, InterruptedException {
        // first parse the jason, then send the reviews
        int numOfReviews = 0;
        int numOfanswers = 0;
        final int MAX_T = 10;
        Ec2Client ec2 = Ec2Client.create();
        String localQueueURL = SendReceiveMessages.getQueueURLByName("localsendqueue");
        Message message;
        int nameCounter = 0;
        ExecutorService pool = Executors.newFixedThreadPool(MAX_T);

        while (!shouldTerminate()) { //default visibilty timeout is 30 seconds. So receive is thread safe
            String workersQueueURL = SendReceiveMessages.createSQS("jobs");
            message = SendReceiveMessages.receive(localQueueURL, "bucket", "key", "n", "id"); //maybe receive many messages.
            if (message != null) {
                SendReceiveMessages.deleteMessage(localQueueURL, message);

                //manage threads



                    Runnable r1 = new ManagerTask(message);


                    // creates a thread pool with MAX_T no. of
                    // threads as the fixed pool size(Step 2)


                    // passes the Task objects to the pool to execute (Step 3)
                    pool.execute(r1);



                    // pool shutdown ( Step 4)
//                int n = Integer.parseInt(SendReceiveMessages.extractAttribute(message, "n"));
//                // download the file
//                String key = SendReceiveMessages.extractAttribute(message, "key");
//                String bucket = SendReceiveMessages.extractAttribute(message, "bucket");
//                String filename = "input" + String.valueOf(nameCounter) + ".txt";
//                String inputPath = "C:\\Users\\yotam\\Desktop\\" + filename;
//                S3ObjectOperations.getObject(key, bucket, inputPath);
//                // parse the message to get the reviews
//                JsonParser parser = new JsonParser(inputPath);
//
//                String workersQueueURL = SendReceiveMessages.createSQS("jobs");
//                List<Review> reviews;
//
//                while (parser.hasNextInput()) {
//                    System.out.println("has next input");
//                    reviews = parser.getNextReviews();
//                    numOfReviews += reviews.size();
//
//                    // create m-k workers
//                    createWorkers(ec2, numOfReviews, n);
//
//                    for (Review review : reviews) {
//                        // send each message twice, once for ner and once for sentiment
//                        distributeJobsToWorkers(review, workersQueueURL);
//                    }
//                }
            }


        }
        terminate(ec2, numOfReviews, numOfanswers, pool);
    }

    public static List<Pair<String,String>> parseLocalMessageLocations(Message message){

        String[] split = message.body().split(":");
        List<Pair<String,String>> locations = new ArrayList<Pair<String,String>>();
        for(int i = 0; i< split.length; i += 2){
            if ( (split.length) % 2 != 0) {
                System.out.println("parsing message, but didnt receive an even number of array elements");
                System.exit(1);
            }

            locations.add(new Pair(split[i], split[i+1]));
        }
        return locations;
    }


    public static void createFile(String filename) {
        try {
            File myObj = new File(filename);
            if (myObj.createNewFile()) {
                System.out.println("File created: " + myObj.getName());
            } else {
                System.out.println("File already exists.");
            }
        } catch (IOException e) {
            System.out.println("An error occurred.");
            e.printStackTrace();
        }
    }

    private static boolean shouldTerminate() {
        Message message;
        String localQueueURL = SendReceiveMessages.getQueueURLByName("localsendqueue");
        message = SendReceiveMessages.receive(localQueueURL, "terminate");
        if (message != null)
            System.out.println("ShouldTerminate: " + SendReceiveMessages.extractAttribute(message, "terminate"));

        if (message != null && SendReceiveMessages.extractAttribute(message, "terminate") != null
                && SendReceiveMessages.extractAttribute(message, "terminate").equals("true")
        ) {
            return true;
        }
        return false;
    }

    public static int countInstances(Ec2Client ec2, String job) {
        System.out.println("countInstances of type " + job);
        int count = 0;
        // Create a Filters to find a running manager/worker
        Filter runningFilter = Filter.builder()
                .name("instance-state-name")
                .values("running")
                .build();

        Filter jobFilter = Filter.builder()
                .name("tag:job")
                .values(job)
                .build();

        //Create a DescribeInstancesRequest
        DescribeInstancesRequest request = DescribeInstancesRequest.builder()
                .filters(jobFilter, runningFilter)
                .build();

        // Find the running job instances
        DescribeInstancesResponse response = ec2.describeInstances(request);

        for (Reservation reservation : response.reservations()) {
            if (!reservation.instances().isEmpty()) {
                count++;
            }
        }

        return count;
    }


    public static void createWorkers(Ec2Client ec2, int numOfReviews, int n) {
        System.out.println("createWorkers, numOfReviews is " + numOfReviews + "and n is" + n);
        int k = countInstances(ec2, "worker");
        int worker_count = numOfReviews / n + 1 - k; // the +1 is for integer division
        for (int i = 0; i < worker_count; i++) {
            String script = "#!/bin/bash\n" +
                    "cd AWS-files\n" +
                    "java -cp 2-AWS-11.jar ass1/Worker\n";
            String val_of_i = String.valueOf(i);
            String[] arguments = {"worker" + val_of_i, "ami-0701529d238b4ec2a", script, "worker"};
            CreateInstance_yotam.main(arguments);
        }
    }

    public static void distributeJobsToWorkers(Review review, String workersQueueURL) {
        final Map<String, MessageAttributeValue> messageAttributes = new HashMap();
        System.out.println("distributeJobsToWorkers:" + review.getId());
        MessageAttributeValue reviewID = SendReceiveMessages.createStringAttributeValue(review.getId());
        messageAttributes.put("reviewId", reviewID);

        MessageAttributeValue NER = SendReceiveMessages.createStringAttributeValue("NER");
        messageAttributes.put("job", NER);

        SendReceiveMessages.send(workersQueueURL, review.getText(), messageAttributes);

        MessageAttributeValue sentiment = SendReceiveMessages.createStringAttributeValue("sentiment");
        messageAttributes.put("job", sentiment);
        SendReceiveMessages.send(workersQueueURL, review.getText(), messageAttributes);

    }

//    public static void splitAndSend(Message msg, String workersQueueURL) {
//        String[] split = msg.body().split(":", 1);
//        if (split.length != 2) {
//            System.out.println("manager didnt receive a message with key and bucket or bucket/key string contanied a colon");
//            System.exit(1);
//        }
//        String bucket = split[0];
//        String key = split[1]; // key is the file path
//        SendReceiveMessages_yotam.send(workersQueueURL, bucket + ':' + key);
//    }

    public static int receiveMessagesFromWorkers(int numberOfAnswers, int numOfMessages) throws IOException {
//        while (true) {
//            // getBucket can also create the bucket
//            String outputBucket = S3ObjectOperations.getBucket("outputs");
//            String answersURL = SendReceiveMessages.getQueueURLByName("answers");
//            Message answer = SendReceiveMessages.receive(answersURL, "reviewID", "job");
//            if (answer != null) {
//                ++numberOfAnswers;
//                if (numOfMessages == numberOfAnswers) {
//                    createFile("answer-not-null");
//                    String key = S3ObjectOperations.PutObject("answer-not-null", outputBucket);
//                    String localRecieveQueueUrl = SendReceiveMessages.getQueueURLByName("loaclRecieveQueue");
//                    final Map<String, MessageAttributeValue> messageAttributes = new HashMap();
//                    MessageAttributeValue KEY = SendReceiveMessages.createStringAttributeValue(key);
//                    messageAttributes.put("key", KEY);
//                    MessageAttributeValue bucket = SendReceiveMessages.createStringAttributeValue(outputBucket);
//                    messageAttributes.put("bucket", bucket);
//                    SendReceiveMessages.send(localRecieveQueueUrl, "", messageAttributes);
//                }
//
//            } else {
//                break;
//            }
//        }

        return numberOfAnswers;
    }

    /*
    the manager "Should not accept any more input files from local applications."
    meaning one local application should be able to terminate the single manager
    and prevent any local apps from running
     */


    public static void terminate(Ec2Client ec2, int numOfReviews, int numberOfanswers, ExecutorService pool) throws InterruptedException, IOException { // stop everything connected to the local that sent the terminate
        System.out.println("terminating");

        int numOfMessages = numOfReviews * 2; //two jobs per review
        while (numOfMessages != numberOfanswers) { //TODO: wait for workers to finish
            sleep(2000);
            numberOfanswers += receiveMessagesFromWorkers(numberOfanswers, numOfMessages);
        }
        pool.shutdown();
        TerminateInstancesRequest terminateRequest = TerminateInstancesRequest.builder().instanceIds(getIntsancesIDsByJob(ec2, "worker")).build();
        ec2.terminateInstances(terminateRequest);

    }

//    public static void stopInstance(String instance_id)
//    {
//        final AmazonEC2 ec2 = AmazonEC2ClientBuilder.defaultClient();
//
//        DryRunSupportedRequest<StopInstancesRequest> dry_request =
//                () -> {
//                    StopInstancesRequest request = new StopInstancesRequest()
//                            .withInstanceIds(instance_id);
//
//                    return request.getDryRunRequest();
//                };

    //terminate EC2 Instances


//        ec2Client.stopInstances(stopInstancesRequest)
//                .getStoppingInstances()
//                .get(0)
//                .getPreviousState()
//                .getName();
//        System.out.println("Stopped the Instnace with ID: "+instanecID);


    public static List<String> getIntsancesIDsByJob(Ec2Client ec2, String job) {
        System.out.println("getIntsancesIDsByJob");
        Filter jobFilter = Filter.builder()
                .name("tag:job")
                .values(job)
                .build();

        //Create a DescribeInstancesRequest
        DescribeInstancesRequest request = DescribeInstancesRequest.builder()
                .filters(jobFilter)
                .build();

        // Find the running job instances and get ids
        DescribeInstancesResponse response = ec2.describeInstances(request);
        List<Instance> instances = new ArrayList<Instance>();
        response.reservations().stream().forEach(reservation -> instances.addAll(reservation.instances()));
        List<String> instancesIds = new ArrayList<String>();
        instances.stream().forEach(instance -> instancesIds.add(instance.instanceId()));
        return instancesIds;
    }


}//Manager








