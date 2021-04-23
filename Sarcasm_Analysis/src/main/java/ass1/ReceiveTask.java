package ass1;

import javafx.util.Pair;
//import jdk.internal.net.http.common.Pair;
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


public class ReceiveTask implements Runnable {

    private int numOfReviews;
    private Message message;
    private Ec2Client ec2;
    private int n;
    private int numOfWorkers;
    private List<String> workersIds;
    // need a list of local ids
    private String localId;
    private Map<String, CloudLocal> locals


    public ReceiveTask(Map<String, CloudLocal> lcls) {
        locals = lcls;
    }
    // 1)make father receive multiple messages (2) split manager task into recive and distribute
    // (3) make threads work on all local aplictaions togther (4) keep local applcaiton in data structure
    //

    @Override
    public void run() {
        try {
            receiveMessagesFromWorkers();
        } catch (Exception e) {
            e.printStackTrace();
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
    

    public void receiveMessagesFromWorkers() throws IOException {
        // getBucket can also create the bucket

        String answersURL = SendReceiveMessages.getQueueURLByName("answers");
        List<Message> answers = SendReceiveMessages.receiveMany(answersURL, 10, "reviewID", "job", "id");

        for (Message ans : answers) {
            String localid = SendReceiveMessages.extractAttribute(ans, "id");
            if (locals.get(localId).updateValuesAndCheckDone(ans)) {
                generateOutputFile(locals.get(localId));
            }
        }
    }




    public static void generateOutputFile(CloudLocal local) throws IOException {
        String localid = local.getId();
        String outputName = "output-" + localid;
        try {
            File myObj = new File(outputName);
            if (myObj.createNewFile()) {
                System.out.println("File created: " + myObj.getName());
            } else {
                System.out.println("File already exists.");
            }
        } catch (IOException e) {
            System.out.println("An error occurred.");
            e.printStackTrace();
        }
        //getBucket also creates the bucket if nessecery
        String outputBucket = S3ObjectOperations.getBucket("outputs-" + localid);

        String key = S3ObjectOperations.PutObject(outputName, outputBucket);
        String localRecieveQueueUrl = SendReceiveMessages.getQueueURLByName("loaclrecievequeue");
        final Map<String, MessageAttributeValue> messageAttributes = new HashMap();
        MessageAttributeValue KEY = SendReceiveMessages.createStringAttributeValue(key);
        messageAttributes.put("key",KEY);
        MessageAttributeValue bucket = SendReceiveMessages.createStringAttributeValue(outputBucket);
        messageAttributes.put("bucket",bucket);
        SendReceiveMessages.send(localRecieveQueueUrl,"",messageAttributes);
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

