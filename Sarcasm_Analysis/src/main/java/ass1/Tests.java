package ass1;

import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.ec2.model.DescribeImagesRequest;
import software.amazon.awssdk.services.ec2.model.DescribeImagesResponse;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.MessageAttributeValue;

import java.util.HashMap;
import java.util.Map;

public class Tests {
    public static void main(String[] args) {
        // send 2 messages to localsendqueue
//        String localQueueURL = SendReceiveMessages.getQueueURLByName("localsendqueue");
//        Map<String, MessageAttributeValue> attributes = new HashMap();
//        SqsClient sqs = SqsClient.builder().region(Region.US_EAST_1).build();
//
//        MessageAttributeValue n = SendReceiveMessages.createStringAttributeValue("100");
//        attributes.put("n", n);
//
//        MessageAttributeValue localID1 = SendReceiveMessages.createStringAttributeValue("1");
//        attributes.put("localID", localID1);
//        SendReceiveMessages.send(localQueueURL, "inputtestttt:input1.txt", attributes);
//
//        MessageAttributeValue localID2 = SendReceiveMessages.createStringAttributeValue("2");
//        attributes.put("localIDl", localID2);
//        SendReceiveMessages.send(localQueueURL, "inputtestttt:input2.txt", attributes);
//        System.out.println("0");
//        Ec2Client ec2 = Ec2Client.create();
//        System.out.println("1");
//        DescribeImagesRequest request = DescribeImagesRequest.builder().build();
//        System.out.println("2");
//        DescribeImagesResponse response = ec2.describeImages(request);
//        System.out.println("3");
//        String ImageID = response.images().get(0).imageId();
//        System.out.println("4");
//        System.out.println("imageId is  " + ImageID);
    }
}
