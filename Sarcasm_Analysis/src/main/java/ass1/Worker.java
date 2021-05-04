package ass1;

import java.util.HashMap;
import java.util.Map;

import software.amazon.awssdk.services.sqs.model.ChangeMessageVisibilityRequest;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.MessageAttributeValue;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageResponse;


public class Worker {

    public static SentimentAnalysisHandler sentimentAnalysisHandler = new SentimentAnalysisHandler();
    public static NamedEntityRecognitionHandler namedEntityRecognitionHandler = new NamedEntityRecognitionHandler();

    public static void main(String[] args) {
        final Map<String, MessageAttributeValue> messageAttributes = new HashMap();

        while (true) {
            String jobQueueURL = "";
            String answersQueueURL = "";
            try {
                jobQueueURL = SendReceiveMessages.getQueueURLByName("jobs");
                answersQueueURL = SendReceiveMessages.getQueueURLByName("answers");
            }catch (Exception e){
                e.printStackTrace();
                continue;
            }
            Message jobMessage = null;
            try {
                jobMessage = SendReceiveMessages.receive(jobQueueURL, "job", "reviewID", "localID");
                if(jobMessage != null) {
                    SendReceiveMessages.changeQueueVisibilityTimeout(jobQueueURL, 60, jobMessage.receiptHandle());
                }

            } catch (Exception e) {
                e.printStackTrace();
                continue;
            }

            if (jobMessage == null) {
                continue;
            }


            String revID = SendReceiveMessages.extractAttribute(jobMessage, "reviewID");
            MessageAttributeValue reviewID = SendReceiveMessages.createStringAttributeValue(revID);
            messageAttributes.put("reviewID", reviewID);

            String job = SendReceiveMessages.extractAttribute(jobMessage, "job");
            MessageAttributeValue jobAttributeValue = SendReceiveMessages.createStringAttributeValue(job);
            messageAttributes.put("job", jobAttributeValue);

            String localID = SendReceiveMessages.extractAttribute(jobMessage, "localID");
            MessageAttributeValue localIDAttributeValue = SendReceiveMessages.createStringAttributeValue(localID);
            messageAttributes.put("localID", localIDAttributeValue);

            if (job.equals("NER")) {

                String ner = namedEntityRecognitionHandler.getEntities(jobMessage.body());
                if (ner.equals("")) {
                    ner = " ";
                }

                try {
                    SendReceiveMessages.send(answersQueueURL,
                            ner,
                            messageAttributes);

                    SendReceiveMessages.deleteMessage(jobQueueURL, jobMessage);
                } catch (Exception e) {
                    e.printStackTrace();
                    continue;
                }
            }

            else if (job.equals("sentiment")) {
                int sentiment = sentimentAnalysisHandler.findSentiment(jobMessage.body());
                try {
                    SendReceiveMessages.send(answersQueueURL,
                            String.valueOf(sentiment),
                            messageAttributes);

                    SendReceiveMessages.deleteMessage(jobQueueURL, jobMessage);
                } catch (Exception e) {
                    e.printStackTrace();
                    continue;
                }
            }
        }
    }
}




