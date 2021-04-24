package ass1;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
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


public class DistributeTask implements Runnable {

    private int numOfReviews;
    private Message message;
    private Ec2Client ec2;
    private int n;
    private int numOfWorkers;
    private List<String> workersIds;
    private CloudLocal local;


    public DistributeTask(Message msg, CloudLocal local) {

        numOfReviews = 0;
        message = msg;
        // local ids is a list of size 1
        ec2 = Ec2Client.create();
        n = 0;
        numOfWorkers = 0;
        workersIds = new ArrayList<String>();
        this.local = local;
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
                String inputPath = "C:\\Users\\yotam\\Desktop\\" + filename;
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
                    createWorkers();

                    for (Review review : reviews) {
                        // send each message twice, once for ner and once for sentiment
                        distributeJobsToWorkers(review, workersQueueURL);
                        local.incNumOfMessages(2);
                    }
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

    public void createWorkers() {
        int k = countInstances(ec2, "worker");
        numOfWorkers = numOfReviews / n + 1 - k; // the +1 is for integer division
        for (int i = 0; i < numOfWorkers; i++) {
            String script = "#!/bin/bash\n" +
                    "cd AWS-files\n" +
                    "java -cp 2-AWS-11.jar ass1/Worker\n";
            String val_of_i = String.valueOf(i);
            String[] arguments = {"worker" + val_of_i, "ami-0701529d238b4ec2a", script, "worker"};
           CreateInstance.main(arguments);
        }
    }

    public static int countInstances(Ec2Client ec2, String job) {
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

}//end of class

