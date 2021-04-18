package ass1;

import java.io.IOException;
import java.io.*;
import java.util.ArrayList;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

public class JsonParser_yotam {
    public static void main(String[] args) throws IOException {

        ArrayList<JSONObject> reviews = new ArrayList<JSONObject>();
        JSONObject obj;
        // The name of the file to open.
        String fileName = "C:\\Users\\yotam\\Desktop\\aws_alon\\Distributed_Systems\\Sarcasm_Analysis\\src\\main\\java\\ass1\\input1.txt ";

        // This will reference one line at a time
        String line = null;
        String workersQueueURL = "123";

        try {
            // FileReader reads text files in the default encoding.
            FileReader fileReader = new FileReader(fileName);

            // Always wrap FileReader in BufferedReader.
            BufferedReader bufferedReader = new BufferedReader(fileReader);

            while ((line = bufferedReader.readLine()) != null) {
                obj = (JSONObject) new JSONParser().parse(line);
                reviews = (ArrayList<JSONObject>) obj.get("reviews");
                for( JSONObject review : reviews){
                    System.out.println(review.get("text"));
                    //SendReceiveMessages.send(workersQueueURL, review.get());
                }

            }
            // Always close files.
            bufferedReader.close();
        } catch (FileNotFoundException ex) {
            System.out.println("Unable to open file '" + fileName + "'");
        } catch (IOException ex) {
            System.out.println("Error reading file '" + fileName + "'");
            // Or we could just do this:
            // ex.printStackTrace();
        } catch (ParseException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }
}
