package ass1;


import software.amazon.awssdk.services.sqs.model.Message;

import java.io.IOException;
import java.util.*;

public class ReceiveTask implements Runnable {

    private Map<String, CloudLocal> locals;


    public ReceiveTask(Map<String, CloudLocal> lcls) {
        locals = lcls;
    }
    // 1)make father receive multiple messages (2) split manager task into recive and distribute
    // (3) make threads work on all local aplictaions togther (4) keep local applcaiton in data structure
    //

    @Override
    public void run() {
        try {
            receiveMessagesFromWorkers();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void receiveMessagesFromWorkers() throws IOException {
        // getBucket can also create the bucket
        String answersURL = SendReceiveMessages.getQueueURLByName("answers");
        List<Message> answers = SendReceiveMessages.receiveMany(answersURL, 10, "reviewID", "job", "localID");

        if (answers.size() > 0) {
            for (Message ans : answers) {
                SendReceiveMessages.deleteMessage(answersURL, ans);
                String localID = SendReceiveMessages.extractAttribute(ans, "localID");
                if (localID != null) {
                    if (locals.get(localID).updateValuesAndCheckDone(ans)) {
                        locals.get(localID).generateOutputFile();
                        locals.get(localID).setDone(true);
                        System.out.println("receive: set done to true");
                   }
                }
            }
        }

    }

}

