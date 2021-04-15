package ass1;

import java.util.HashMap;
import java.util.Map;

import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.MessageAttributeValue;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageResponse;


public class Worker {

    public static SentimentAnalysisHandler sentimentAnalysisHandler = new SentimentAnalysisHandler();
    public static NamedEntityRecognitionHandler namedEntityRecognitionHandler = new NamedEntityRecognitionHandler();
    private static boolean stop;

    public static void main(String[] args) {

        // 1) read (some) message from the queue
        // 2) Perform the requsted job, and return the result
        // 3) remove the processed message from the SQS queue
        // send (some) message describes the original review location, together
        // with the out put of the operation.

        ReceiveMessageResponse receiveMessageResponse;
for(int i = 0  ; i < 10 ; i++ ){
    System.out.println(String.valueOf(i));
//        while (!shouldStop()) {
            String jobQueueURL = SendReceiveMessages.getQueueURLByName("jobs");
            String answersQueueURL = SendReceiveMessages.getQueueURLByName("answers");

            Message jobMessage = SendReceiveMessages.receive(jobQueueURL, "job","reviewID");

            if (jobMessage == null) {
                System.out.println("no message in queue");
                continue;
            }

            String job = SendReceiveMessages.extractAttribute(jobMessage, "job");
            if(job == null) {
                System.out.println("nabcsndlkasndlka");
            }


            final Map<String, MessageAttributeValue> messageAttributes = new HashMap();

            MessageAttributeValue reviewID = SendReceiveMessages.createStringAttributeValue(String.valueOf(jobMessage.attributes().get("reviewId")));
            messageAttributes.put("reviewId", reviewID);

            MessageAttributeValue jobAttributeValue = SendReceiveMessages.createStringAttributeValue(job);
            messageAttributes.put("job", jobAttributeValue);

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




