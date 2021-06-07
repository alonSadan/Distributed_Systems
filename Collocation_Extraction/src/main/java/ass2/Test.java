package ass2;

import java.io.IOException;
import java.net.URISyntaxException;

public class Test{
    public static void main(String[] args) throws IOException, URISyntaxException {
        S3ObjectOperations.getObject("s3://datasets.elasticmapreduce/ngrams/books/20090715/eng-gb-all/1gram/data");
        System.out.println("hi");
    }
}