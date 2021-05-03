//snippet-sourcedescription:[CreateInstance.java demonstrates how to create an EC2 instance.]
//snippet-keyword:[SDK for Java 2.0]
//snippet-keyword:[Code Sample]
//snippet-service:[ec2]
//snippet-sourcetype:[full-example]
//snippet-sourcedate:[11/02/2020]
//snippet-sourceauthor:[scmacdon]
/*
 * Copyright 2010-2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *  http://aws.amazon.com/apache2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */
package ass1;

import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.ec2.model.*;

import java.util.Base64;

/**
 * Creates an EC2 instance
 */
public class CreateInstance {

    public static void main(String[] args) {
        final String USAGE =
                "usage:  java -jar local-1.0-jar-with-dependencies.jar inputFileName1... inputFileNameN outputFileName1... outputFileNameN n [terminate]";

        if (args.length != 3 && args.length != 4) {
            System.out.println(USAGE);
            System.exit(1);
        }

        String name = args[0];
        String amiId = args[1];

        String script = args[2];
        String job = "";
        if (args.length == 4)
            job = args[3];

        //InstanceProfileCredentialsProvider provider = InstanceProfileCredentialsProvider.builder().build();
        Ec2Client ec2 = Ec2Client.builder().region(Region.US_EAST_1).build();
        RunInstancesRequest runRequest = RunInstancesRequest.builder()
                .instanceType(InstanceType.T2_SMALL)
                .imageId(amiId)
                .iamInstanceProfile(IamInstanceProfileSpecification.builder().arn("arn:aws:iam::855350177051:instance-profile/ass1full").build())
                .maxCount(1)
                .minCount(1)
                .userData(Base64.getEncoder().encodeToString(script.getBytes()))
                .build();

        RunInstancesResponse response = ec2.runInstances(runRequest);

        String instanceId = response.instances().get(0).instanceId();

        addTag("Name", name, instanceId, amiId, ec2);

        if (job != "") {
            addTag("job", job, instanceId, amiId, ec2);
        }

        System.out.println("Instance created!");
    }

    public static void addTag(String key, String value, String instanceId, String amiId, Ec2Client ec2) {
        Tag tag = Tag.builder()
                .key(key)
                .value(value)
                .build();

        CreateTagsRequest tagRequest = CreateTagsRequest.builder()
                .resources(instanceId)
                .tags(tag)
                .build();

        try {
            ec2.createTags(tagRequest);
            System.out.printf(
                    "Successfully added tag with key: %s and value: %s\n",
                    key, value);

        } catch (Ec2Exception e) {
            System.err.println(e.getMessage());
            System.exit(1);
        }
    }

}