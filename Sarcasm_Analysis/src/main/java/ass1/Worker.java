package ass1;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import java.io.FileReader;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.ec2.model.*;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.MessageAttributeValue;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageResponse;


public class Worker {

    private static SentimentAnalysisHandler sentimentAnalysisHandler = new SentimentAnalysisHandler();
    private static NamedEntityRecognitionHandler namedEntityRecognitionHandler = new NamedEntityRecognitionHandler();
    private static boolean stop;

    public static void main(String[] args) {

        // 1) read (some) message from the queue
        // 2) Perform the requsted job, and return the result
        // 3) remove the processed message from the SQS queue
        // send (some) message describes the original review location, together
        // with the out put of the operation.

        ReceiveMessageResponse receiveMessageResponse;

        while (!shouldStop()) {
            String jobQueueURL = SendReceiveMessages.getQueueURLByName("JOB_QUEUE");
            String answersQueueURL = SendReceiveMessages.getQueueURLByName("ANSWERS_QUEUE");

            Message jobMessage = SendReceiveMessages.receive(jobQueueURL, "job");

            if (jobMessage == null) {
                System.err.println("no message in queue");
                continue;
            }

            String job = SendReceiveMessages.extractAttribute(jobMessage, "job");
            final Map<String, MessageAttributeValue> messageAttributes = new HashMap();

            MessageAttributeValue reviewID = SendReceiveMessages.createStringAttributeValue(jobMessage.attributes().get("reviewId"));
            messageAttributes.put("reviewId", reviewID);

            MessageAttributeValue jobAttributeValue = SendReceiveMessages.createStringAttributeValue(job);
            messageAttributes.put("job", jobAttributeValue);

            if (job == "NER") {

                String ner = namedEntityRecognitionHandler.getEntities(jobMessage.body());
                SendReceiveMessages.send(answersQueueURL,
                        ner,
                        messageAttributes);

                SendReceiveMessages.deleteMessage(jobQueueURL, jobMessage);
            }

            if (job == "sentiment") {
                SendReceiveMessages.send(answersQueueURL,
                        "sentiment defined in a message attribute with key 'sentiment'",
                        messageAttributes);

                SendReceiveMessages.deleteMessage(jobQueueURL, jobMessage);
            }
        }
    }

    private static boolean shouldStop() {
        Message workerMessage;
        String wokerQueueURL = SendReceiveMessages.getQueueURLByName("WORKER_QUEUE");
        workerMessage = SendReceiveMessages.receive(wokerQueueURL);
        if (workerMessage != null && SendReceiveMessages.extractAttribute(workerMessage, "stop") == "true") {
            return true;
        }
        return false;
    }


}




