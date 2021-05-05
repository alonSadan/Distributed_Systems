package ass1;

import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model. *;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.DeleteQueueRequest;

import java.util.List;

public class ClearS3AndSQS {

    public static void main (String [] args) {
        clear ();
    }

    public static void clear () {
        S3Client s3Client = S3Client.builder ().build ();
        SqsClient sqsClient = SqsClient.builder ().build ();
        for (String url: sqsClient.listQueues (). queueUrls ()) {
            sqsClient.deleteQueue (DeleteQueueRequest.builder (). queueUrl (url) .build ());
        }
        for (Bucket bucket: s3Client.listBuckets (). buckets ()) {
            List <S3Object> objects = s3Client.listObjectsV2(ListObjectsV2Request.builder()
                    .bucket(bucket.name ())
                    .build ())
                    .contents() ;
            for (S3Object object: objects) {
                try {
                    s3Client.deleteObject (DeleteObjectRequest.builder (). bucket (bucket.name ()). key (object.key ()). build ());
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
            s3Client.deleteBucket (DeleteBucketRequest.builder (). bucket (bucket.name ()). build ());
        }
    }
}