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

    // default constructor to avoid Exception
    public Review(){}

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

}


