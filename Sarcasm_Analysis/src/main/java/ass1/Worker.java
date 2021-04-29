package ass1;

import java.util.HashMap;
import java.util.Map;

import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.MessageAttributeValue;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageResponse;


public class Worker {

    public static SentimentAnalysisHandler sentimentAnalysisHandler = new SentimentAnalysisHandler();
    public static NamedEntityRecognitionHandler namedEntityRecognitionHandler = new NamedEntityRecognitionHandler();

    public static void main(String[] args) {

        while (true) {
            String jobQueueURL = SendReceiveMessages.getQueueURLByName("jobs");
            String answersQueueURL = SendReceiveMessages.getQueueURLByName("answers");

            Message jobMessage = SendReceiveMessages.receive(jobQueueURL, "job", "reviewID", "localID");

            if (jobMessage == null) {
                continue;
            }

            final Map<String, MessageAttributeValue> messageAttributes = new HashMap();

            String revID = SendReceiveMessages.extractAttribute(jobMessage, "reviewID");
            MessageAttributeValue reviewID = SendReceiveMessages.createStringAttributeValue(revID);
            messageAttributes.put("reviewId", reviewID);

            String job = SendReceiveMessages.extractAttribute(jobMessage, "job");
            MessageAttributeValue jobAttributeValue = SendReceiveMessages.createStringAttributeValue(job);
            messageAttributes.put("job", jobAttributeValue);

            String localID = SendReceiveMessages.extractAttribute(jobMessage, "localID");
            MessageAttributeValue localIDAttributeValue = SendReceiveMessages.createStringAttributeValue(localID);
            messageAttributes.put("localID", jobAttributeValue);

            if (job.equals("NER")) {

                String ner = namedEntityRecognitionHandler.getEntities(jobMessage.body());
                SendReceiveMessages.send(answersQueueURL,
                        ner,
                        messageAttributes);

                SendReceiveMessages.deleteMessage(jobQueueURL, jobMessage);
            }

            if (job.equals("sentiment")) {
                int sentiment = sentimentAnalysisHandler.findSentiment(jobMessage.body());
                SendReceiveMessages.send(answersQueueURL,
                        String.valueOf(sentiment),
                        messageAttributes);

                SendReceiveMessages.deleteMessage(jobQueueURL, jobMessage);
            }
        }
    }
}




