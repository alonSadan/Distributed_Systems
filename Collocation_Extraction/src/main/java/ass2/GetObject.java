package ass2;

import java.io.IOException;
import java.net.URISyntaxException;

public class GetObject{
    public static void main(String[] args) throws IOException, URISyntaxException {
        getObject("s3://datasets.elasticmapreduce/ngrams/books/20090715/eng-gb-all/1gram/data");
    }

    public static void getObject(String bucketURL) throws IOException, URISyntaxException {
        S3ObjectOperations.getObject(bucketURL);
        System.out.println("done");
    }
}