package ass1;

import static j2html.TagCreator.*;

import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.MessageAttributeValue;
import org.apache.commons.io.FileUtils;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

public class CloudLocal {
    private String id;
    private int numOfAnswers;
    private int numOfMessages;
    private AtomicBoolean done;
    private Map<String, Review> reviews;


    public static void main(String[] args) throws IOException {
        CloudLocal cl = new CloudLocal("alon");
        String inputPath = "C:\\Users\\alons\\studies\\distributed_systems\\Distributed_Systems\\Sarcasm_Analysis\\input files\\testInput.txt";
        JsonParser parser = new JsonParser(inputPath);
        List<Review> rvws;
        while (parser.hasNextInput()) {
            rvws = parser.getNextReviews();
            cl.setReviewsFromList(rvws);
        }

        for(Review review: cl.getReviews().values()){
            review.setSentiment(3);
            review.setNamedEntityRecognition("[ALON:PERSON;LACHISH:PLACE]");
        }
        cl.generateOutputFile();

    }

    public CloudLocal(String ID) {
        id = ID;
        numOfAnswers = 0;
        numOfMessages = 0;
        done = new AtomicBoolean(false);
        reviews = new HashMap<String, Review>();
    }

    public String getId() {
        return id;
    }

    public synchronized boolean updateValuesAndCheckDone(Message message) {
        updateReviews(message);
        numOfAnswers++;
        return done.getAndSet(numOfAnswers == numOfMessages);
    }

    public boolean isDone() {
        return done.get();
    }

    public synchronized void incNumOfMessages(int numOfMessages) {
        this.numOfMessages += numOfMessages;
    }


    public void updateReviews(Message message) {
        String reviewID = SendReceiveMessages.extractAttribute(message, "reviewID");
        String job = SendReceiveMessages.extractAttribute(message, "job");
        if (job.equals("sentiment")) {
            reviews.get(reviewID).setSentiment(Integer.parseInt(message.body()));
        } else {
            reviews.get(reviewID).setNamedEntityRecognition(message.body());
        }
    }

    public void setReviewsFromList(List<Review> revws) {
        for (Review review : revws) {
            reviews.put(review.getId(), review);
        }
    }

    public  void generateOutputFile() throws IOException {
        String outputName = "output-" + id + ".html";
        File output = createOutputFileToCWD(outputName);
        renderHTMLToFile(output);
        uploadOutputFileToS3(outputName);
    }

    public File createOutputFileToCWD(String outputName){
        try {
            File output = new File(outputName);

//            if(!output.setWritable(true,false)){
//                if(!output.canWrite()) {
//                    System.out.println("No permissions to set file as writeable");
//                    System.exit(1);
//                }
//            }
            if (output.createNewFile()) {
                System.out.println("File created: " + output.getName());
            } else {
                System.out.println("File already exists.");
            }
            return output;
        } catch (IOException e) {
            System.out.println("An error occurred.");
            e.printStackTrace();
        }

        return null;
    }

    public void uploadOutputFileToS3(String outputName) throws IOException {
        //getBucket also creates the bucket if nessecery
        String outputBucket = S3ObjectOperations.getBucket("outputs-" + id);

        String key = S3ObjectOperations.PutObject(outputName, outputBucket);
        String localRecieveQueueUrl = SendReceiveMessages.getQueueURLByName("loaclrecievequeue");
        final Map<String, MessageAttributeValue> messageAttributes = new HashMap();

        MessageAttributeValue KEY = SendReceiveMessages.createStringAttributeValue(key);
        messageAttributes.put("key",KEY);
        MessageAttributeValue bucket = SendReceiveMessages.createStringAttributeValue(outputBucket);
        messageAttributes.put("bucket",bucket);
        SendReceiveMessages.send(localRecieveQueueUrl,"",messageAttributes);
    }

    public void renderHTMLToFile(File file) throws IOException {
        String[] colors  = {"Darkred","red","black","green","Darkgreen"};
       // Appendable writer = new FileWriter(fileName,true);
      String html =   html(
                head(
                        title("output")
                ),
                body(
                        each(reviews.values(), rev -> p(
                                span("Link: " + rev.getLink()).withStyle("color:"+colors[rev.getSentiment()]),
                                span("|").withStyle("font-weight:bold;font-size:30px"),
                                span("Named-Entity: "+rev.getNamedEntityRecognition()),
                                span("|").withStyle("font-weight:bold;font-size:30px"),
                                span("Sarcastic: " + rev.isSarcastic())
                        )))
        ).renderFormatted();
        FileUtils.writeStringToFile(file, html);
    }

    public Map<String, Review> getReviews() {
        return reviews;
    }
}

