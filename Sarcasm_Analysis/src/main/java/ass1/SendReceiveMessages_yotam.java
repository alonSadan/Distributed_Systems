package ass1;

import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.*;

import java.util.Date;
import java.util.List;

// snippet-end:[sqs.java2.send_recieve_messages.import]
// snippet-start:[sqs.java2.send_recieve_messages.main]
public class SendReceiveMessages_yotam {
    private static final String QUEUE_NAME = "testQueue" + new Date().getTime();
    private static final SqsClient sqs = SqsClient.builder().region(Region.US_EAST_1).build();

    public static void send(String queueUrl, String messageBody) {

        SendMessageRequest send_msg_request = SendMessageRequest.builder()
                .queueUrl(queueUrl)
                .messageBody(messageBody)
                .delaySeconds(5)
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

    public static List<Message> receive(String queueUrl) {
        ReceiveMessageRequest receiveRequest = ReceiveMessageRequest.builder()
                .queueUrl(queueUrl)
                .build();
        List<Message> messages = sqs.receiveMessage(receiveRequest).messages();
        return messages;
    }

    public static String getQueueURL(String Q_name){
        GetQueueUrlRequest getQueueRequest = GetQueueUrlRequest.builder()
                .queueName(Q_name)
                .build();

        return sqs.getQueueUrl(getQueueRequest).queueUrl();
    }


    public static String createSQS(String name) {
        SqsClient sqs = SqsClient.builder().region(Region.US_EAST_1).build();

        try {
            CreateQueueRequest request = CreateQueueRequest.builder()
                    .queueName(name)
                    .build();
            CreateQueueResponse create_result = sqs.createQueue(request);
        } catch (QueueNameExistsException e) {
            throw e;

        }

        GetQueueUrlRequest getQueueRequest = GetQueueUrlRequest.builder()
                .queueName(name)
                .build();
        String queueUrl = sqs.getQueueUrl(getQueueRequest).queueUrl();
        return queueUrl;
    }
        // delete messages from the queue
//        for (Message m : messages) {
//           DeleteMessageRequest deleteRequest = DeleteMessageRequest.builder()
//                 .queueUrl(queueUrl)
//                 .receiptHandle(m.receiptHandle())
//                 .build();
//            sqs.deleteMessage(deleteRequest);
//        }
    } //end of class
