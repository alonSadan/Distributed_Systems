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
package ass2;
// snippet-start:[s3.java2.s3_object_operations.complete]
// snippet-start:[s3.java2.s3_object_operations.import]

import com.amazonaws.services.s3.AmazonS3URI;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.core.sync.ResponseTransformer;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;
import software.amazon.awssdk.services.s3.paginators.ListObjectsV2Iterable;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.SdkClientException;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.TransferManagerBuilder;
import com.amazonaws.services.s3.transfer.Upload;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;
import java.nio.file.Paths;
import java.util.Date;
import java.util.List;
import java.util.Random;


public class S3ObjectOperations {

    private static S3Client s3;

    public S3ObjectOperations() {
        Region region = Region.US_EAST_1;
        s3 = S3Client.builder().region(region).build();
    }

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
    public static void getObject(String bucket, String key, String output) throws IOException {
        Region region = Region.US_EAST_1;
        s3 = S3Client.builder().region(region).build();
        s3.getObject(GetObjectRequest.builder().bucket(bucket).key(key).build(),
                ResponseTransformer.toFile(Paths.get(output)));
    }

    public static void getObject(String bucketURL) throws IOException, URISyntaxException {
        AmazonS3URI amazonURI = new AmazonS3URI(bucketURL);
        String bucket = amazonURI.getBucket();
        String key = amazonURI.getKey();
        getObject(bucket,key,String.valueOf(new Date().getTime()));
    }

    public static boolean isObjectExistsOnS3(String bucket, String key) throws IOException {
        Region region = Region.US_EAST_1;
        s3 = S3Client.builder().region(region).build();
        ListObjectsRequest request = ListObjectsRequest.builder().bucket(bucket).prefix(key).build();
        return !s3.listObjects(request).contents().isEmpty();
    }

    public static String CreateBucket(String bucket) {
        Region region = Region.US_EAST_1;
        s3 = S3Client.builder().region(region).build();
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

    public static String getBucket(String bucket) {
        List<Bucket> buckets = s3.listBuckets().buckets();
        for (Bucket b : buckets) {
            if (b.name().equals(bucket))
                return bucket;
        }
        return CreateBucket(bucket);
    }

    private static void deleteBucket(String bucket) {
        DeleteBucketRequest deleteBucketRequest = DeleteBucketRequest.builder().bucket(bucket).build();
        s3.deleteBucket(deleteBucketRequest);
    }


    private static void multipartUpload(String bucketName, String key) throws IOException {

        int mb = 1024 * 1024;
        // First create a multipart upload and get upload id
        CreateMultipartUploadRequest createMultipartUploadRequest = CreateMultipartUploadRequest.builder()
                .bucket(bucketName).key(key)
                .build();
        CreateMultipartUploadResponse response = s3.createMultipartUpload(createMultipartUploadRequest);
        String uploadId = response.uploadId();

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