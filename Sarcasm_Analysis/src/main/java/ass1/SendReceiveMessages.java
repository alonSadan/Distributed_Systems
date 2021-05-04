package ass1;

import software.amazon.awssdk.auth.credentials.InstanceProfileCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.*;

import java.util.*;

import static java.util.Collections.emptyList;

// snippet-end:[sqs.java2.send_recieve_messages.import]
// snippet-start:[sqs.java2.send_recieve_messages.main]
public class SendReceiveMessages {

    public static void send(String queueUrl, String messageBody, Map<String, MessageAttributeValue> attributes) {
        //InstanceProfileCredentialsProvider provider = InstanceProfileCredentialsProvider.builder().build();
        SqsClient sqs = SqsClient.builder().region(Region.US_EAST_1)
                //credentialsProvider(provider).
                .build();

        SendMessageRequest send_msg_request = SendMessageRequest.builder()
                .queueUrl(queueUrl)
                .messageBody(messageBody)
                .messageAttributes(attributes)
                .build();
        sqs.sendMessage(send_msg_request);

    }

    public static void changeQueueVisibilityTimeout(String queueURL, int timeout,String recipt){
        //InstanceProfileCredentialsProvider provider = InstanceProfileCredentialsProvider.builder().build();
        SqsClient sqs = SqsClient.builder().region(Region.US_EAST_1)
                //credentialsProvider(provider).
                .build();

        ChangeMessageVisibilityRequest req =ChangeMessageVisibilityRequest.builder()
                .queueUrl(queueURL)
                .visibilityTimeout(timeout)
                .receiptHandle(recipt)
                .build();
        sqs.changeMessageVisibility(req);
    }
    public static List<Message> receiveMany(String queueUrl, int maxNumberOfMessages, String... attributeNames) { //returns one message by default
//        InstanceProfileCredentialsProvider provider = InstanceProfileCredentialsProvider.builder().build();
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
        //InstanceProfileCredentialsProvider provider = InstanceProfileCredentialsProvider.builder().build();
        SqsClient sqs = SqsClient.builder().region(Region.US_EAST_1).build();


        CreateQueueRequest request = CreateQueueRequest.builder()
                .queueName(name)
                .build();

        return sqs.createQueue(request).queueUrl();
    }

    public static String getQueueURLByName(String name) {
        //InstanceProfileCredentialsProvider provider = InstanceProfileCredentialsProvider.builder().build();
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
//        InstanceProfileCredentialsProvider provider = InstanceProfileCredentialsProvider.builder().build();
        SqsClient sqs = SqsClient.builder().region(Region.US_EAST_1).build();

        DeleteMessageRequest deleteRequest = DeleteMessageRequest.builder()
                .queueUrl(queueURL)
                .receiptHandle(message.receiptHandle())
                .build();
        sqs.deleteMessage(deleteRequest);
    }
} //end of class
