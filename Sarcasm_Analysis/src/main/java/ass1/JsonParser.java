package ass1;


import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;

public class JsonParser {

    private final Iterator<Input> inputIterator;

    public JsonParser(String path) throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        JsonFactory jsonFactory = new JsonFactory();
        BufferedReader br = new BufferedReader(new FileReader(path));
        inputIterator = mapper.readValues(jsonFactory.createParser(br), Input.class);
    }

    //main is an example on how to use class json parser;
    public static void main(String[] args) throws IOException {

        //ObjectMapper objectMapper = new ObjectMapper();
        //Input iput = objectMapper.readValue(new File("example.json"), Input.class);
        String pathToinput = "C:\\Users\\yotam\\Desktop\\aws_alon\\Distributed_Systems\\Sarcasm_Analysis\\src\\main\\java\\ass1\\input1.txt";
        JsonParser parser;
        try {
            parser = new JsonParser(pathToinput);
        } catch (IOException e) {
            e.printStackTrace();
            return;
        }

        //example to print 3 reviews and than another 3
//        int i = 0;
//        while (parser.hasNextInput()) {
//            System.out.println(parser.getNextInput());
//            ++i;
//            if (i == 3) {
//                brels sr  ak;
//            }
//        }
//
//        i = 0;
//        while (parser.hasNextInput()) {
//            System.out.println(parser.getNextInput());
//            ++i;
//            if (i == 3) {
//                break;
//            }
//        }
        //example to get and print all authors of reviews in a single input.
        while (parser.hasNextInput()) {
            List<Review> reviews = parser.getNextReviews();
            Iterator<Review> reviewsIterator = reviews.iterator();
            while (reviewsIterator.hasNext()) {
                System.out.println(reviewsIterator.next().getAuthor());
            }
        }
    }

    public Input getNextInput() {
        if (this.inputIterator.hasNext()) {
            return inputIterator.next();
        }
        return null;
    }

    public boolean hasNextInput() {
        return inputIterator.hasNext();
    }

    public List<Review> getNextReviews() {
        if (hasNextInput()) {
            return getNextInput().getReviews();
        }

        return null;
    }

}