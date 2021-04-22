package ass1;

import jdk.internal.net.http.common.Pair;
import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.ec2.model.*;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.MessageAttributeValue;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

import static java.lang.Thread.sleep;
import static java.util.Collections.emptyList;


public class ManagerTask implements Runnable {

    private int numOfReviews;
    private Message message;
    private Ec2Client ec2;
    private int n;
    private int numOfWorkers;
    private List<String> workersIds;
    private String localId;


    public ManagerTask(Message msg) {

        numOfReviews = 0;
        message = msg;
        localId = SendReceiveMessages.extractAttribute(msg,"id");
        ec2 = Ec2Client.create();
        n = 0;
        numOfWorkers = 0;
        workersIds = new ArrayList<String>();
    }


    @Override
    public void run() {
        try {
            n = Integer.parseInt(SendReceiveMessages.extractAttribute(message, "n"));
            // download the file
            List<Pair<String, String>> locations = parseLocalMessageLocations(message);
            for (Pair<String, String> location : locations) {   //PAir<Key, Bucket>
                String filename = "input" + new Date().getTime();
                String inputPath = "C:\\Users\\yotam\\Desktop\\" + filename;
                S3ObjectOperations.getObject(location.first, location.second, inputPath);
                // parse the message to get the reviews
                JsonParser parser = new JsonParser(inputPath);
                String workersQueueURL = SendReceiveMessages.getQueueURLByName("jobs");
                List<Review> reviews;

                while (parser.hasNextInput()) {
                    System.out.println("has next input");
                    reviews = parser.getNextReviews();
                    numOfReviews += reviews.size();

                    // create m-k workers
                    createWorkers();

                    for (Review review : reviews) {
                        // send each message twice, once for ner and once for sentiment
                        distributeJobsToWorkers(review, workersQueueURL);
                    }
                }
            }

            receiveMessagesFromWorkers();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static List<Pair<String, String>> parseLocalMessageLocations(Message message) {

        String[] split = message.body().split(":");
        List<Pair<String, String>> locations = new ArrayList<Pair<String, String>>();
        for (int i = 0; i < split.length; i += 2) {
            if ((split.length) % 2 != 0) {
                System.out.println("parsing message, but didnt receive an even number of array elements");
                System.exit(1);
            }

            locations.add(new Pair(split[i], split[i + 1]));
        }
        return locations;
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

    public void createWorkers() {
        System.out.println("createWorkers, numOfReviews is " + numOfReviews + "and n is" + n);
        int k = countInstances(ec2, "worker");
        numOfWorkers = numOfReviews / n + 1 - k; // the +1 is for integer division
        for (int i = 0; i < numOfWorkers; i++) {
            String script = "#!/bin/bash\n" +
                    "cd AWS-files\n" +
                    "java -cp 2-AWS-11.jar ass1/Worker\n";
            String val_of_i = String.valueOf(i);
            String[] arguments = {"worker" + val_of_i, "ami-0701529d238b4ec2a", script, "worker"};
            workersIds.add(CreateInstance_yotam.main(arguments));
        }
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

    public void terminate(int numberOfanswers) throws InterruptedException, IOException { // stop everything connected to the local that sent the terminate
        System.out.println("terminating");
        //TODO:stop the other manager threads
        int numOfMessages = numOfReviews * 2; //two jobs per review
//        while (numOfMessages != numberOfanswers) { //TODO: wait for workers to finish
//            sleep(2000);
//            numberOfanswers += receiveMessagesFromWorkers(numberOfanswers, numOfMessages);
//        }
        TerminateInstancesRequest terminateRequest = TerminateInstancesRequest
                .builder()
                .instanceIds(getIntsancesIDsByJob("worker", numOfWorkers))
                .build();
        ec2.terminateInstances(terminateRequest);
    }

    public void receiveMessagesFromWorkers() throws IOException {
        int numOfAnswers = 0;
        int numOfMessages = numOfReviews * 2;
        // getBucket can also create the bucket
        String outputBucket = S3ObjectOperations.getBucket("outputs-" + localId);
        String answersURL = SendReceiveMessages.getQueueURLByName("answers");
        while (numOfAnswers != numOfMessages) {
            Message answer = SendReceiveMessages.receive(answersURL, "reviewID", "job", localId);
            if (answer != null) {
                ++numOfAnswers;
            }
        }
        String outputName = "output" + localId;
        createFile(outputName);
        String key = S3ObjectOperations.PutObject(outputName, outputBucket);
        String localRecieveQueueUrl = SendReceiveMessages.getQueueURLByName("loaclrecievequeue");
        final Map<String, MessageAttributeValue> messageAttributes = new HashMap();
        MessageAttributeValue KEY = SendReceiveMessages.createStringAttributeValue(key);
        messageAttributes.put("key", KEY);
        MessageAttributeValue bucket = SendReceiveMessages.createStringAttributeValue(outputBucket);
        messageAttributes.put("bucket", bucket);
        SendReceiveMessages.send(localRecieveQueueUrl, "", messageAttributes);
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

    public List<String> getIntsancesIDsByJob(String job, List<String> workersIds) {
        System.out.println("getIntsancesIDsByJob");
        Filter jobFilter = Filter.builder()
                .name("tag:job")
                .values(job)
                .build();

        //Create a DescribeInstancesRequest
        DescribeInstancesRequest request = DescribeInstancesRequest.builder()
                .filters(jobFilter)
                .instanceIds(workersIds)
                .build();

        // Find the running job instances and get ids
        DescribeInstancesResponse response = ec2.describeInstances(request);
        List<Instance> instances = new ArrayList<Instance>();
        response.reservations().stream().forEach(reservation ->
                instances.addAll(reservation.instances()));
        List<String> instancesIds = new ArrayList<String>();
        instances.stream().forEach(instance -> instancesIds.add(instance.instanceId()));
        return instancesIds;
    }
}

