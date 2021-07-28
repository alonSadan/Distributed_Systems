package ass2;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.Scanner;

public class fileContainsWord {
    private File f;

    public fileContainsWord(String fileName) {
        f = new File(fileName);
    }

//    public static void main(String[] args) throws Exception {
//        f  = new File("C:\\Users\\alons\\studies\\distributed_systems\\Distributed_Systems\\Collocation_Extraction\\stopWords.txt");
//        System.out.println(contains("ב"));
//    }

    public boolean contains(String word) throws FileNotFoundException {
        try {
            Scanner scanner = new Scanner(f);
            int lineNum = 0;
            while (scanner.hasNextLine()) {
                String line = scanner.nextLine();
                lineNum++;
                if (line.equals(word)) {
                    return true;
                }
            }
            scanner.close();
        } catch (
                FileNotFoundException e) {
            e.printStackTrace();
        }

        return false;
    }
}
