package ass1;

import java.util.List;
import java.util.Properties;

import edu.stanford.nlp.ling.CoreAnnotations;
import edu.stanford.nlp.ling.CoreAnnotations.NamedEntityTagAnnotation;
import edu.stanford.nlp.ling.CoreAnnotations.SentencesAnnotation;
import edu.stanford.nlp.ling.CoreAnnotations.TextAnnotation;
import edu.stanford.nlp.ling.CoreAnnotations.TokensAnnotation;
import edu.stanford.nlp.ling.CoreLabel;
import edu.stanford.nlp.pipeline.Annotation;
import edu.stanford.nlp.pipeline.StanfordCoreNLP;
import edu.stanford.nlp.rnn.RNNCoreAnnotations;
import edu.stanford.nlp.sentiment.SentimentCoreAnnotations;
import edu.stanford.nlp.trees.Tree;
import edu.stanford.nlp.util.CoreMap;
import edu.stanford.nlp.util.logging.RedwoodConfiguration;

public class NamedEntityRecognitionHandler {

    public String getEntities(String review) {
        Annotation document;
        StanfordCoreNLP NERPipeline;
        Properties props;

        props = new Properties();
        props.put("annotators", "tokenize,ssplit,pos,lemma,ner");
        NERPipeline = new StanfordCoreNLP(props);
        // create an empty Annotation just with the given text
        document = new Annotation(review);
        // run all Annotators on this text
        NERPipeline.annotate(document);
        // these are all the sentences in this document
        // a CoreMap is essentially a Map that uses class objects as keys and has values with custom types
        List<CoreMap> sentences = document.get(SentencesAnnotation.class);

        String entities = "";
        for (CoreMap sentence : sentences) {
            // traversing the words in the current sentence
            // a CoreLabel is a CoreMap with additional token-specific methods
            for (CoreLabel token : sentence.get(TokensAnnotation.class)) {
                // this is the text of the token
                String word = token.get(TextAnnotation.class);
                // this is the NER label of the token
                String ne = token.get(NamedEntityTagAnnotation.class);
                if(!ne.equals("O")) {
                    System.out.println("ne is :" + ne);
                    entities = entities + (word + ":" + ne + ", ");
                }
            }
        }

        // return result string without the last space and comma
        return entities.substring(0, entities.length() - 2);
    }
}