package ass1;

import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.*;

import java.util.*;

// snippet-end:[sqs.java2.send_recieve_messages.import]
// snippet-start:[sqs.java2.send_recieve_messages.main]
public class SendReceiveMessages {
    //    private static final String QUEUE_NAME = "testQueue" + new Date().getTime()

    ////// inputtestttt
    public static void main(String[] args) {
        String localQueueURL = getQueueURLByName("localsendqueue");
        Map<String, MessageAttributeValue> attributes = new HashMap();
        SqsClient sqs = SqsClient.builder().region(Region.US_EAST_1).build();
        MessageAttributeValue n = SendReceiveMessages.createStringAttributeValue("100");
        attributes.put("n", n);

        MessageAttributeValue key = SendReceiveMessages.createStringAttributeValue("input1.txt");
        attributes.put("key", key);

        MessageAttributeValue bucket = SendReceiveMessages.createStringAttributeValue("inputtestttt");
        attributes.put("bucket", bucket);
        send(localQueueURL, "blah", attributes);

    }
    

    public static void send(String queueUrl, String messageBody, Map<String, MessageAttributeValue> attributes) {
        SqsClient sqs = SqsClient.builder().region(Region.US_EAST_1).build();
        SendMessageRequest send_msg_request = SendMessageRequest.builder()
                .queueUrl(queueUrl)
                .messageBody(messageBody)
                .messageAttributes(attributes)
                .build();
        sqs.sendMessage(send_msg_request);
    }
    
    public static List<Message> receiveMany(String queueUrl, int maxNumberOfMessages, String... attributeNames) { //returns one message by default
        SqsClient sqs = SqsClient.builder().region(Region.US_EAST_1).build();
        ReceiveMessageRequest receiveRequest = ReceiveMessageRequest.builder()
                .messageAttributeNames(attributeNames)
                .maxNumberOfMessages(maxNumberOfMessages)
                .queueUrl(queueUrl)
                .build();
        List<Message> allmessages = sqs.receiveMessage(receiveRequest).messages();
        List<Message> filteredMessages = filterMessagesByAttributes(allmessages, attributeNames);

        return filteredMessages;
    }

    public static Message receive(String queueUrl, String... attributeNames) { //returns one message by default
        List<Message> optioanlMessage = receiveMany(queueUrl, 1, attributeNames);
        if (optioanlMessage != null && !optioanlMessage.isEmpty()) {
            return optioanlMessage.get(0);
        }
        return null;
    }

    public static List<Message> filterMessagesByAttributes(List<Message> messages, String... attributeNames) {
        List<Message> filteredMessages = new ArrayList<Message>();
        boolean takeMessage;
        for (Message message : messages) {
            takeMessage = true;
            for (String name : attributeNames) {
                if (extractAttribute(message, name) == null) {
                    takeMessage = false;
                    break;
                }
            }
            if (takeMessage) {
                filteredMessages.add(message);
            }
        }
        return filteredMessages;
    }

    public static String createSQS(String name) {
        SqsClient sqs = SqsClient.builder().region(Region.US_EAST_1).build();

        CreateQueueRequest request = CreateQueueRequest.builder()
                .queueName(name)
                .build();

        return sqs.createQueue(request).queueUrl();
    }

    public static String getQueueURLByName(String name) {
        SqsClient sqs = SqsClient.builder().region(Region.US_EAST_1).build();
        try {
            GetQueueUrlRequest getQueueRequest = GetQueueUrlRequest.builder()
                    .queueName(name)
                    .build();
            return sqs.getQueueUrl(getQueueRequest).queueUrl();
        } catch (QueueDoesNotExistException e) { //queue doesnt exist
            return createSQS(name);
        }
    }


    public static String extractAttribute(Message message, String attributeName) {
        if (!message.messageAttributes().isEmpty()) {
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
        SqsClient sqs = SqsClient.builder().region(Region.US_EAST_1).build();
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
