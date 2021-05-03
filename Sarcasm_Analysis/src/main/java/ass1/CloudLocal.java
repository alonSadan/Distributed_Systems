package ass1;

import static j2html.TagCreator.*;
import static java.lang.Thread.sleep;

import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.MessageAttributeValue;
import org.apache.commons.io.FileUtils;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

public class CloudLocal {
    private String id;
    private int numOfAnswers;
    private int numOfMessages;
    private AtomicBoolean done;
    private Map<String, Review> reviews;

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
        uploadOutputFileToS3AndDelete(outputName);
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

    public void uploadOutputFileToS3AndDelete(String outputName) throws IOException {
        //getBucket also creates the bucket if nessecery
        String outputBucket = S3ObjectOperations.getBucket("outputs-" + id);

        String key = S3ObjectOperations.PutObject(outputName, outputBucket);
        String localRecieveQueueUrl = SendReceiveMessages.getQueueURLByName("loaclrecievequeue");
        final Map<String, MessageAttributeValue> messageAttributes = new HashMap();

        MessageAttributeValue KEY = SendReceiveMessages.createStringAttributeValue(key);
        messageAttributes.put("key",KEY);
        MessageAttributeValue bucket = SendReceiveMessages.createStringAttributeValue(outputBucket);
        messageAttributes.put("bucket",bucket);
        MessageAttributeValue localID = SendReceiveMessages.createStringAttributeValue(getId());
        messageAttributes.put("localID",localID);

        SendReceiveMessages.send(localRecieveQueueUrl," ",messageAttributes);

        deleteAfterUpload(outputName, outputBucket, key);
    }

    public void deleteAfterUpload(String outputPath, String bucket, String key) throws IOException {
        while(! S3ObjectOperations.isObjectExistsOnS3(bucket, key)){

        }
        Path path = Paths.get(outputPath);
        try {
            Files.deleteIfExists(path);
        } catch (IOException e) {
            e.printStackTrace();
        }
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

