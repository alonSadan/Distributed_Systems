package ass1;

import jdk.internal.net.http.common.Pair;

import java.util.ArrayList;
import java.util.List;

public class CloudLocal {
    private long id;
    private List<Pair<String, String>> inputFilesLocations; // key and bucket

    public CloudLocal(long ID){
        id = ID;
        inputFilesLocations = new ArrayList<>();
    }

    public long getId() {
        return id;
    }

    public void addFile(String key, String bucket) {
        inputFilesLocations.add(new Pair<>(key, bucket));
    }

    public List<Pair<String, String>> getInputFilesLocations() {
        return inputFilesLocations;
    }
}
