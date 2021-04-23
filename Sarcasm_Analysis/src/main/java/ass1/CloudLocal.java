package ass1;

import javafx.util.Pair;
import software.amazon.awssdk.services.sqs.model.Message;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

public class CloudLocal {
    private String id;
    private List<Pair<String, String>> inputFilesLocations; // key and bucket
    private int numOfAnswers;
    private int numOfMessages;
    private AtomicBoolean done;

    public CloudLocal(String ID){
        id = ID;
        numOfAnswers = 0;
        numOfMessages = 0;
        done = new AtomicBoolean(false);
    }

    public String getId() {
        return id;
    }

    public synchronized boolean updateValuesAndCheckDone(Message message){
        numOfAnswers++;
        return done.getAndSet(numOfAnswers == numOfMessages);
    }

    public boolean isDone(){
        return done.get();
    }

    public synchronized void incNumOfMessages(int numOfMessages) {
        this.numOfMessages += numOfMessages;
    }
}
