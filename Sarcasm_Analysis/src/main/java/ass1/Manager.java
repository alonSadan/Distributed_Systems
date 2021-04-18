package ass1;

import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.ec2.model.*;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.MessageAttributeValue;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static java.lang.Thread.sleep;

import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.AmazonEC2ClientBuilder;
import com.amazonaws.services.ec2.model.DryRunResult;
import com.amazonaws.services.ec2.model.DryRunSupportedRequest;
import com.amazonaws.services.ec2.model.StartInstancesRequest;
import com.amazonaws.services.ec2.model.StopInstancesRequest;

// - "mvn package" creates a jar file
// - running "java -cp 2-files-test-1.0-SNAPSHOT.jar Hello" prints hello
public class Manager {
    public static void main(String[] args) throws IOException {
        int numOfReviews = 0;
        int numOfanswers = 0;
        Ec2Client ec2 = Ec2Client.create();
        String localQueueURL = SendReceiveMessages.getQueueURLByName("localsendqueue");
            do {
                Message message = SendReceiveMessages.receive(localQueueURL, "bucket", "key", "n");
                // current working directory = System.getProperty("user.dir")
                if (message != null) {
                    int n = Integer.parseInt(SendReceiveMessages.extractAttribute(message, "n"));
                    // download the file
                    String key = SendReceiveMessages.extractAttribute(message, "key");
                    String bucket = SendReceiveMessages.extractAttribute(message, "bucket");
                    S3ObjectOperations.getObject(key, bucket, "/home/ec2-user/AWS-files/input.txt");
                    // parse the message to get the reviews
                    JsonParser parser = new JsonParser("/home/ec2-user/AWS-files/input.txt");

                    String workersQueueURL = SendReceiveMessages.createSQS("jobs");
                    List<Review> reviews;

                    while (parser.hasNextInput()) {
                        reviews = parser.getNextReviews();
                        numOfReviews += reviews.size();

                        // create m-k workers
                        createWorkers(ec2, numOfReviews, n);

                        for (Review review : reviews) {
                            // send each message twice, once for ner and once for sentiment
                            distributeJobsToWorkers(review, workersQueueURL);
                        }
                    }

                    SendReceiveMessages.deleteMessage(localQueueURL, message);
                }

                numOfanswers  += receiveMessagesFromWorkers();

            } while (!shouldTerminate());
        terminate(ec2, numOfReviews, numOfanswers);
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
        message = SendReceiveMessages.receive(localQueueURL);
        if (message != null && SendReceiveMessages.extractAttribute(message, "terminate") == "true") {
            return true;
        }
        return false;
    }

    public static int countInstances(Ec2Client ec2, String job) {
        int count = 0;
        // Create a Filters to find a running manager/worker
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
                count++;
            }
        }

        return count;
    }


    public static void createWorkers(Ec2Client ec2, int numOfReviews, int n) {
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

        MessageAttributeValue reviewID = SendReceiveMessages.createStringAttributeValue(review.getId());
        messageAttributes.put("reviewId", reviewID);

        MessageAttributeValue NER = SendReceiveMessages.createStringAttributeValue("NER");
        messageAttributes.put("job", NER);

        SendReceiveMessages.send(workersQueueURL, review.getText(), messageAttributes);

        MessageAttributeValue sentiment = SendReceiveMessages.createStringAttributeValue("sentiment");
        messageAttributes.put("job", sentiment);
        SendReceiveMessages.send(workersQueueURL, review.getText(), messageAttributes);

    }

    public static void splitAndSend(Message msg, String workersQueueURL) {
        String[] split = msg.body().split(":", 1);
        if (split.length != 2) {
            System.out.println("manager didnt receive a message with key and bucket or bucket/key string contanied a colon");
            System.exit(1);
        }
        String bucket = split[0];
        String key = split[1]; // key is the file path
        SendReceiveMessages_yotam.send(workersQueueURL, bucket + ':' + key);
    }

    public static int receiveMessagesFromWorkers() throws IOException {
        int numberOfAnswers = 0;
        while (true) {
            String outputBucket = S3ObjectOperations.CreateBucket("outputs");
            String answersURL = SendReceiveMessages.getQueueURLByName("answers");
            Message answer = SendReceiveMessages.receive(answersURL, "reviewID", "job");
            if (answer != null) {
                ++numberOfAnswers;
                createFile("answer-not-null");
                String key = S3ObjectOperations.PutObject("answer-not-null", outputBucket);
                String localRecieveQueueUrl = SendReceiveMessages.getQueueURLByName("loaclRecieveQueue");

                final Map<String, MessageAttributeValue> messageAttributes = new HashMap();
                MessageAttributeValue KEY = SendReceiveMessages.createStringAttributeValue(key);
                messageAttributes.put("key", KEY);
                MessageAttributeValue bucket = SendReceiveMessages.createStringAttributeValue(outputBucket);
                messageAttributes.put("bucket", bucket);

                SendReceiveMessages.send(localRecieveQueueUrl, "", messageAttributes);
            } else {
                break;
            }
        }

        return numberOfAnswers;
    }

    public static void terminate(Ec2Client ec2, int numOfReviews, int numberOfanswers) throws InterruptedException { // stop everything connected to the local that sent the terminate
        int numOfMessages = numOfReviews * 2; //two jobs per review
        while (numOfMessages != numberOfanswers){ //TODO: wait for workers to finish
            sleep(2000);
        }
        int numOfWorkers = countInstances(ec2,"worker");
        System.exit(0);
    }

    public static void stopInstance(String instance_id)
    {
        final AmazonEC2 ec2 = AmazonEC2ClientBuilder.defaultClient();

        DryRunSupportedRequest<StopInstancesRequest> dry_request =
                () -> {
                    StopInstancesRequest request = new StopInstancesRequest()
                            .withInstanceIds(instance_id);

                    return request.getDryRunRequest();
                };

}











