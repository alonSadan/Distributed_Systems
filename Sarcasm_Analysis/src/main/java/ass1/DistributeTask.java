package ass1;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.ec2.model.*;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.MessageAttributeValue;

import java.io.File;
import java.nio.file.Files;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

import static java.lang.Thread.sleep;
import static java.util.Collections.emptyList;


public class DistributeTask implements Runnable {

    private int numOfReviews;
    private Message message;
    private Ec2Client ec2;
    private int n;
    private int numOfWorkers;
    private CloudLocal local;
    private final ReentrantLock lock;

    public DistributeTask(Message msg, CloudLocal local, ReentrantLock lock) {

        numOfReviews = 0;
        message = msg;
        // local ids is a list of size 1
        ec2 = Ec2Client.create();
        n = 0;
        numOfWorkers = 0;
        this.local = local;
        this.lock = lock;
    }
    // 1)make father receive multiple messages (2) split manager task into recive and distribute
    // (3) mkae threads work on all local aplictaions togther (4) keep local applcaiton in data structure
    //

    @Override
    public void run() {
        try {
            n = Integer.parseInt(SendReceiveMessages.extractAttribute(message, "n"));
            // download the file
            List<Pair<String, String>> locations = parseLocalMessageLocations(message);
            for (Pair<String, String> location : locations) {   //PAir<bucket, key>
                String filename = "input" + new Date().getTime();
                String inputPath = System.getProperty("user.dir") + File.separator + filename;
                S3ObjectOperations.getObject(location.getKey(), location.getValue(), inputPath);
                // parse the message to get the reviews
                JsonParser parser = new JsonParser(inputPath);
                String workersQueueURL = SendReceiveMessages.getQueueURLByName("jobs");
                List<Review> reviews;

                while (parser.hasNextInput()) {
                    reviews = parser.getNextReviews();
                    local.setReviewsFromList(reviews);
                    numOfReviews += reviews.size();

                    // create m-k workers
                    //createWorkers();

                    for (Review review : reviews) {
                        // send each message twice, once for ner and once for sentiment
                        distributeJobsToWorkers(review, workersQueueURL);
                        local.incNumOfMessages(2);
                    }
                }
                Path path = Paths.get(inputPath);
                try {
                    Files.deleteIfExists(path);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static List<Pair<String, String>> parseLocalMessageLocations(Message message) {

        String[] split = message.body().split(":");
        List<Pair<String, String>> locations = new ArrayList<Pair<String, String>>();
        for (int i = 0; i < split.length; i += 2) {
            if ((split.length) % 2 != 0) {
                System.err.println("parsing message, but didnt receive an even number of array elements");
                System.exit(1);
            }

            locations.add(new ImmutablePair<>(split[i], split[i + 1]));
        }
        return locations;
    }

    public void distributeJobsToWorkers(Review review, String workersQueueURL) {
        final Map<String, MessageAttributeValue> messageAttributes = new HashMap();
        MessageAttributeValue reviewID = SendReceiveMessages.createStringAttributeValue(review.getId());
        messageAttributes.put("reviewID", reviewID);

        MessageAttributeValue localID = SendReceiveMessages.createStringAttributeValue(local.getId());
        messageAttributes.put("localID", localID);

        MessageAttributeValue NER = SendReceiveMessages.createStringAttributeValue("NER");
        messageAttributes.put("job", NER);

        SendReceiveMessages.send(workersQueueURL, review.getText(), messageAttributes);

        MessageAttributeValue sentiment = SendReceiveMessages.createStringAttributeValue("sentiment");
        messageAttributes.put("job", sentiment);
        SendReceiveMessages.send(workersQueueURL, review.getText(), messageAttributes);
    }

    public void createWorkers() {
        String ImageID = "ami-065d6a9a537cdad1d";
        lock.lock();
        // we count the workers every input file, so failed initialized workers will be created again soon
        int k = countInstances(ec2, "worker");
        numOfWorkers = numOfReviews / n + 1 - k; // the +1 is for integer division
        for (int i = 0; i < numOfWorkers; i++) {
            String script = "#! /bin/bash\n" +
                    "java -jar /home/ec2-user/worker-1.0-jar-with-dependencies.jar\n";
            String val_of_i = String.valueOf(i);
            String[] arguments = {"worker" + val_of_i, ImageID, script, "worker"};
            CreateInstance.main(arguments);
        }
        lock.unlock();
    }

    public static int countInstances(Ec2Client ec2, String job) {
        int count = 0;
        // Create a Filters to find workers
        Filter jobFilter = Filter.builder()
                .name("tag:job")
                .values(job)
                .build();

        //Create a DescribeInstancesRequest
        DescribeInstancesRequest request = DescribeInstancesRequest.builder()
                .filters(jobFilter)
                .build();

        // Find the filtered job instances
        DescribeInstancesResponse response = ec2.describeInstances(request);

        for (Reservation reservation : response.reservations()) {
            if (!reservation.instances().isEmpty()) {
                count++;
            }
        }

        return count;
    }

}//end of class

