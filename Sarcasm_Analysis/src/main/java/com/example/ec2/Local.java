package ass1;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import java.io.FileReader;
import java.util.Iterator;

import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.ec2.model.*;


public class Local {
    public static void main(String[] args) {
        if (args.length < 1) {
            System.out.println("no input files");
            System.exit(1);
        }

        Ec2Client ec2 = Ec2Client.create();
        if(!managerExists(ec2)){
            CreateManager();
        }
        else{
            S3ObjectOperations.main();
        }


        JSONParser parser = new JSONParser();
        try {
            Object obj = parser.parse(new FileReader(args[0]));

            // A JSON object. Key value pairs are unordered. JSONObject supports java.util.Map interface.
            JSONObject jsonObject = (JSONObject) obj;

            // A JSON array. JSONObject supports java.util.List interface.
            JSONArray companyList = (JSONArray) jsonObject.get("Company List");

            // An iterator over a collection. Iterator takes the place of Enumeration in the Java Collections Framework.
            // Iterators differ from enumerations in two ways:
            // 1. Iterators allow the caller to remove elements from the underlying collection during the iteration with well-defined semantics.
            // 2. Method names have been improved.
            Iterator<JSONObject> iterator = companyList.iterator();
            while (iterator.hasNext()) {
                System.out.println(iterator.next());
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void CreateManager() {
        String [] args = {"manager", "ami-0062dd78ec1ecd019"};
        CreateInstance.main(args);
    }

    public static boolean managerExists(Ec2Client ec2) {

    // Create a Filters to find a running manager

    Filter runningFilter = Filter.builder()
    .name("instance-state-name")
    .values("running")
    .build();

    Filter managerFilter  = Filter.builder()
    .name("tag:job")
    .values("manager")
    .build();

    //Create a DescribeInstancesRequest
    DescribeInstancesRequest request = DescribeInstancesRequest.builder()
        .filters(managerFilter,runningFilter)
        .build();

    // Find the running manager instances
    DescribeInstancesResponse response = ec2.describeInstances(request);




    }

}
