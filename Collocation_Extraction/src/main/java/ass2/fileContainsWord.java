package ass2;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

public class fileContainsWord {
    private File f;
    private List<String> words;

    public fileContainsWord(String fileName) {
        f = new File(fileName);
        words = new ArrayList<>();
        try {
            Scanner scanner = new Scanner(f);
            int lineNum = 0;
            while (scanner.hasNextLine()) {
                String line = scanner.nextLine();
                lineNum++;
                words.add(line);
            }
            scanner.close();
        } catch (FileNotFoundException fileNotFoundException) {
            fileNotFoundException.printStackTrace();
        }

    }


//    public static void main(String[] args) throws Exception {
//        f  = new File("C:\\Users\\alons\\studies\\distributed_systems\\Distributed_Systems\\Collocation_Extraction\\stopWords.txt");
//        System.out.println(contains("ב"));
//    }

    public boolean contains(String word) throws FileNotFoundException {
        return words.contains(word);
    }
}
