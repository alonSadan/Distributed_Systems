package ass1;


import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

import software.amazon.awssdk.auth.credentials.InstanceProfileCredentialsProvider;
import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.ec2.model.*;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.regions.Region;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.locks.ReentrantLock;

import static java.lang.Thread.sleep;

public class Manager {
    public static void main(String[] args) throws IOException, InterruptedException {
        ReentrantLock lock = new ReentrantLock(); // synchronize createWorkers() to prevent too many workers
        final int MAX_T = 18;
        InstanceProfileCredentialsProvider provider = InstanceProfileCredentialsProvider.builder().build();
        Ec2Client ec2 = Ec2Client.builder().credentialsProvider(provider).region(Region.US_EAST_1).build();

        String localQueueURL = SendReceiveMessages.getQueueURLByName("localsendqueue");
        ExecutorService pool = Executors.newFixedThreadPool(MAX_T);
        Map<String, CloudLocal> locals = new HashMap<String, CloudLocal>();

        while (!shouldTerminate()) { //default visibilty timeout is 30 seconds. So receive is thread safe
            createDistributeTasks(localQueueURL, locals, lock, pool);
            createReceiveTasks(locals, pool);
        }
        terminate(ec2, pool, locals);
    }

    public static void createReceiveTasks(Map<String, CloudLocal> locals, ExecutorService pool) {
        if (!locals.isEmpty()) {
            for (int i = 0; i < 2; i++) {
                Runnable r2 = new ReceiveTask(locals);
                pool.execute(r2);
            }
        }
    }

    public static void createDistributeTasks(String localQueueURL, Map<String, CloudLocal> locals, ReentrantLock lock, ExecutorService pool) {
        List<Message> distributeMessages = SendReceiveMessages.receiveMany(localQueueURL, 10, "n", "localID");

        if (!distributeMessages.isEmpty()) {
            for (Message message : distributeMessages) {
                String localid = SendReceiveMessages.extractAttribute(message, "localID");
                locals.put(localid, new CloudLocal(localid)); // each local sends a message only once
                Runnable r1 = new DistributeTask(message, locals.get(localid), lock);
                SendReceiveMessages.deleteMessage(localQueueURL, message);
                pool.execute(r1);
            }
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
            locations.add(new ImmutablePair<>(split[i], split[i + 1]));
        }
        return locations;
    }

    private static boolean shouldTerminate() {
        Message message;
        String localQueueURL = SendReceiveMessages.getQueueURLByName("localsendqueue");
        message = SendReceiveMessages.receive(localQueueURL, "terminate");

        if (message != null && SendReceiveMessages.extractAttribute(message, "terminate") != null
                && SendReceiveMessages.extractAttribute(message, "terminate").equals("true")
        ) {
            SendReceiveMessages.deleteMessage(localQueueURL, message);
            return true;
        }
        return false;
    }


    public static void terminate(Ec2Client ec2, ExecutorService pool, Map<String, CloudLocal> locals) throws InterruptedException, IOException { // stop everything connected to the local that sent the terminate
        Collection<CloudLocal> localsValues = locals.values();

        for (CloudLocal local : localsValues) {
            if (local.isDone()) {
                continue;
            } else {
                while (!local.isDone()) {
                    sleep(2000);
                }
            }
        }
        pool.shutdown();
        TerminateInstancesRequest terminateRequest = TerminateInstancesRequest.builder().instanceIds(getInstancesIDsByJob(ec2, "worker")).build();
        ec2.terminateInstances(terminateRequest);

        ClearS3AndSQS.clear();

        try {
            terminateRequest = TerminateInstancesRequest.builder().instanceIds(getInstancesIDsByJob(ec2, "manager")).build();
            ec2.terminateInstances(terminateRequest);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static List<String> getInstancesIDsByJob(Ec2Client ec2, String job) {
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


}








