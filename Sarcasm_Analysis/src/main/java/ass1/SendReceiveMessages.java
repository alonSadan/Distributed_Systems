package ass1;

import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.*;

import java.util.Date;
import java.util.List;
import java.util.Map;

// snippet-end:[sqs.java2.send_recieve_messages.import]
// snippet-start:[sqs.java2.send_recieve_messages.main]
public class SendReceiveMessages {
    //    private static final String QUEUE_NAME = "testQueue" + new Date().getTime();
    private static final SqsClient sqs = SqsClient.builder().region(Region.US_EAST_1).build();

    public static void send(String queueUrl, String messageBody, Map<String, MessageAttributeValue> attributes) {

        SendMessageRequest send_msg_request = SendMessageRequest.builder()
                .queueUrl(queueUrl)
                .messageBody(messageBody)
                .messageAttributes(attributes)
                .build();
        sqs.sendMessage(send_msg_request);
    }
//        // Send multiple messages to the queue
//        SendMessageBatchRequest send_batch_request = SendMessageBatchRequest.builder()
//                .queueUrl(queueUrl)
//                .entries(
//                        SendMessageBatchRequestEntry.builder()
//                                .messageBody("Hello from message 1")
//                                .delaySeconds(1)
//                                .id("msg_1")
//                                .build()
//                        ,
//                        SendMessageBatchRequestEntry.builder()
//                                .messageBody("Hello from message 2")
//                                .delaySeconds(10)
//                                .id("msg_2")
//                                .build())
//                .build();
//        sqs.sendMessageBatch(send_batch_request);

    public static Message receive(String queueUrl, String... attributeNames) { //returns one message by default
        ReceiveMessageRequest receiveRequest = ReceiveMessageRequest.builder()
                .messageAttributeNames(attributeNames)
                .queueUrl(queueUrl)
                .build();
        List<Message> messages = sqs.receiveMessage(receiveRequest).messages();
        if (!messages.isEmpty()) {
            return messages.get(0);
        }

        return null;
    }


    public static String createSQS(String name) {
        SqsClient sqs = SqsClient.builder().region(Region.US_EAST_1).build();

        CreateQueueRequest request = CreateQueueRequest.builder()
                .queueName(name)
                .build();

        return sqs.createQueue(request).queueUrl();
    }

    public static String getQueueURLByName(String name) {

        GetQueueUrlRequest getQueueRequest = GetQueueUrlRequest.builder()
                .queueName(name)
                .build();
        return sqs.getQueueUrl(getQueueRequest).queueUrl();
    }


    public static String extractAttribute(Message message, String attributeName) {
        if (!message.attributes().isEmpty()) {
            Map<String, MessageAttributeValue> messageAttributes = message.messageAttributes();
            MessageAttributeValue attributeValue = messageAttributes.get(attributeName);
            if (attributeValue != null) {
                return attributeValue.stringValue();
            }
        }
        return null;
    }


    public static MessageAttributeValue createStringAttributeValue(String value) {
        return MessageAttributeValue
                .builder()
                .dataType("String")
                .stringValue(value)
                .build();
    }

    public static void deleteMessage(String queueURL, Message message) {
        DeleteMessageRequest deleteRequest = DeleteMessageRequest.builder()
                .queueUrl(queueURL)
                .receiptHandle(message.receiptHandle())
                .build();
        sqs.deleteMessage(deleteRequest);
    }

    // delete messages from the queue
//        for (Message m : messages) {
//            System.out.println(m.body());
//           DeleteMessageRequest deleteRequest = DeleteMessageRequest.builder()
//                 .queueUrl(queueUrl)
//                 .receiptHandle(m.receiptHandle())
//                 .build();
//            sqs.deleteMessage(deleteRequest);
//        }
} //end of class
