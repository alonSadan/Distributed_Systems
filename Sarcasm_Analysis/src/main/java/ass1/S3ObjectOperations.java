/*
 * Copyright 2011-2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at:
 *
 *    http://aws.amazon.com/apache2.0
 *
 * This file is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES
 * OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and
 * limitations under the License.
 */
package ass1;
// snippet-start:[s3.java2.s3_object_operations.complete]
// snippet-start:[s3.java2.s3_object_operations.import]

import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.core.sync.ResponseTransformer;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;
import software.amazon.awssdk.services.s3.paginators.ListObjectsV2Iterable;


//import com.amazonaws.AmazonServiceException;
//import com.amazonaws.SdkClientException;
//import com.amazonaws.auth.profile.ProfileCredentialsProvider;
//import com.amazonaws.regions.Regions;
//import com.amazonaws.services.s3.AmazonS3;
//import com.amazonaws.services.s3.AmazonS3ClientBuilder;
//import com.amazonaws.services.s3.transfer.TransferManager;
//import com.amazonaws.services.s3.transfer.TransferManagerBuilder;
//import com.amazonaws.services.s3.transfer.Upload;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Paths;
import java.util.Date;
import java.util.Random;


public class S3ObjectOperations {

    private static S3Client s3;

    public static String[] PutObjects(String[] files, String bucketName) throws IOException {
        String keys[] = new String[files.length];
        for (int i = 0; i < files.length; ++i) {
            keys[i] = PutObject(files[i], bucketName);
        }

        return keys;
    }

    public static String PutObject(String filePath, String bucket) throws IOException {
        Region region = Region.US_EAST_1;
        s3 = S3Client.builder().region(region).build();

        String fileName = filePath; //"C:\\Users\\yotam\\Desktop\\aws_alon\\Distributed_Systems\\Sarcasm_Analysis\\input files\\B000EVOSE4.txt";
        String key = String.valueOf(new Date().getTime());

        // Put Object
        s3.putObject(PutObjectRequest.builder().bucket(bucket).key(key)
                        .build(),
                RequestBody.fromFile(new File(fileName)));

        return key;
    }

    // Get Object
    public static void getObject(String key, String bucket, String output) throws IOException {
       s3.getObject(GetObjectRequest.builder().bucket(bucket).key(key).build(),
                ResponseTransformer.toFile(Paths.get(output)));
    }

//
//
////        // Multipart Upload a file
////        String multipartKey = "multiPartKey";
////        multipartUpload(bucket, multipartKey);
//
//        // List all objects in bucket
//
//        // Use manual pagination
//        ListObjectsV2Request listObjectsReqManual = ListObjectsV2Request.builder()
//                .bucket(bucket)
//                .maxKeys(1)
//                .build();
//
//        boolean done = false;
//        while (!done) {
//            ListObjectsV2Response listObjResponse = s3.listObjectsV2(listObjectsReqManual);
//            for (S3Object content : listObjResponse.contents()) {
//                System.out.println(content.key());
//            }
//
//            if (listObjResponse.nextContinuationToken() == null) {
//                done = true;
//            }
//
//            listObjectsReqManual = listObjectsReqManual.toBuilder()
//                    .continuationToken(listObjResponse.nextContinuationToken())
//                    .build();
//        }
//        // Build the list objects request
//        ListObjectsV2Request listReq = ListObjectsV2Request.builder()
//                .bucket(bucket)
//                .maxKeys(1)
//                .build();
//
//        ListObjectsV2Iterable listRes = s3.listObjectsV2Paginator(listReq);
//        // Process response pages
//        listRes.stream()
//                .flatMap(r -> r.contents().stream())
//                .forEach(content -> System.out.println(" Key: " + content.key() + " size = " + content.size()));
//
//        // Helper method to work with paginated collection of items directly
//        listRes.contents().stream()
//                .forEach(content -> System.out.println(" Key: " + content.key() + " size = " + content.size()));
//        // Use simple for loop if stream is not necessary
//        for (S3Object content : listRes.contents()) {
//            System.out.println(" Key: " + content.key() + " size = " + content.size());
//        }
//
//        // Get Object
//        s3.getObject(GetObjectRequest.builder().bucket(bucket).key(key).build(),
//                ResponseTransformer.toFile(Paths.get("multiPartKey")));
    // snippet-end:[s3.java2.s3_object_operations.download]
//
//        // Delete Object
//        DeleteObjectRequest deleteObjectRequest = DeleteObjectRequest.builder().bucket(bucket).key(key).build();
//        s3.deleteObject(deleteObjectRequest);
//
//        // Delete Object
//        deleteObjectRequest = DeleteObjectRequest.builder().bucket(bucket).key("multipartKey").build();
//        s3.deleteObject(deleteObjectRequest);
//
//        deleteBucket(bucket);


    public static String CreateBucket(String bucket) {

        bucket = bucket + System.currentTimeMillis();
        s3.createBucket(CreateBucketRequest
                .builder()
                .bucket(bucket)
                .createBucketConfiguration(
                        CreateBucketConfiguration.builder()
                                //.locationConstraint(region.id())
                                .build())
                .build());

        return bucket;
    }

    private static void deleteBucket(String bucket) {
        DeleteBucketRequest deleteBucketRequest = DeleteBucketRequest.builder().bucket(bucket).build();
        s3.deleteBucket(deleteBucketRequest);
    }

    /**
     * Uploading an object to S3 in parts
     */
    private static void multipartUpload(String bucketName, String key) throws IOException {

        int mb = 1024 * 1024;
        // First create a multipart upload and get upload id 
        CreateMultipartUploadRequest createMultipartUploadRequest = CreateMultipartUploadRequest.builder()
                .bucket(bucketName).key(key)
                .build();
        CreateMultipartUploadResponse response = s3.createMultipartUpload(createMultipartUploadRequest);
        String uploadId = response.uploadId();
        System.out.println(uploadId);

        // Upload all the different parts of the object
        UploadPartRequest uploadPartRequest1 = UploadPartRequest.builder().bucket(bucketName).key(key)
                .uploadId(uploadId)
                .partNumber(1).build();
        String etag1 = s3.uploadPart(uploadPartRequest1, RequestBody.fromByteBuffer(getRandomByteBuffer(5 * mb))).eTag();
        CompletedPart part1 = CompletedPart.builder().partNumber(1).eTag(etag1).build();

        UploadPartRequest uploadPartRequest2 = UploadPartRequest.builder().bucket(bucketName).key(key)
                .uploadId(uploadId)
                .partNumber(2).build();
        String etag2 = s3.uploadPart(uploadPartRequest2, RequestBody.fromByteBuffer(getRandomByteBuffer(3 * mb))).eTag();
        CompletedPart part2 = CompletedPart.builder().partNumber(2).eTag(etag2).build();


        // Finally call completeMultipartUpload operation to tell S3 to merge all uploaded
        // parts and finish the multipart operation.
        CompletedMultipartUpload completedMultipartUpload = CompletedMultipartUpload.builder().parts(part1, part2).build();
        CompleteMultipartUploadRequest completeMultipartUploadRequest =
                CompleteMultipartUploadRequest.builder().bucket(bucketName).key(key).uploadId(uploadId)
                        .multipartUpload(completedMultipartUpload).build();
        s3.completeMultipartUpload(completeMultipartUploadRequest);
    }

    private static ByteBuffer getRandomByteBuffer(int size) throws IOException {
        byte[] b = new byte[size];
        new Random().nextBytes(b);
        return ByteBuffer.wrap(b);
    }
}