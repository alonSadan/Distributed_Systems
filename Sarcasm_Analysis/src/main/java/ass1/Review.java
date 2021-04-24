package ass1;

import com.fasterxml.jackson.annotation.JsonProperty;

public class Review {
    private String id;
    private String link;
    private  String text;
    private  int rating;
    private  String title;
    private String author;
    private  String date;
    private int sentiment = 0;
    private String namedEntityRecognition = "";

    // default constructor to avoid Exception
    public Review(){
    }

    @JsonProperty(value="date")
    public String getDate() {
        return date;
    }

    @JsonProperty(value="id")
    public String getId() {
        return id;
    }

    @JsonProperty(value="rating")
    public int getRating() {
        return rating;
    }
    @JsonProperty(value="link")
    public String getLink() {
        return link;
    }

    @JsonProperty(value="text")
    public String getText() {
        return text;
    }
    @JsonProperty(value="title")
    public String getTitle() {
        return title;
    }

    @JsonProperty(value="author")
    public String getAuthor() {
        return author;
    }


    public void setNamedEntityRecognition(String namedEntityRecognition) {
        this.namedEntityRecognition = namedEntityRecognition;
    }

    public void setSentiment(int sentiment) {
        this.sentiment = sentiment;
    }

    public String getNamedEntityRecognition() {
        return namedEntityRecognition;
    }

    public int getSentiment() {
        return sentiment;
    }

    public boolean isSarcastic(){
        return rating != sentiment;
    }
}


