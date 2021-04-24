package ass1;

import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.MessageAttributeValue;
import java.util.HashMap;
import java.util.Map;

public class Tests {
    public static void main(String[] args) {
        // send 2 messages to localsendqueue
        String localQueueURL = SendReceiveMessages.getQueueURLByName("localsendqueue");
        Map<String, MessageAttributeValue> attributes = new HashMap();
        SqsClient sqs = SqsClient.builder().region(Region.US_EAST_1).build();

        MessageAttributeValue n = SendReceiveMessages.createStringAttributeValue("100");
        attributes.put("n", n);

        MessageAttributeValue localID1 = SendReceiveMessages.createStringAttributeValue("1");
        attributes.put("localID", localID1);
        SendReceiveMessages.send(localQueueURL, "inputtestttt:input1.txt", attributes);

        MessageAttributeValue localID2 = SendReceiveMessages.createStringAttributeValue("2");
        attributes.put("localID", localID2);
        SendReceiveMessages.send(localQueueURL, "inputtestttt:input2.txt", attributes);
    }
}
