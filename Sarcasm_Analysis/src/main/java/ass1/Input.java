package ass1;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

public class Input {
    private String title;
    private List<Review> reviews;


    public Input(){}

    @JsonProperty(value="reviews")
    public List<Review> getReviews() {
        return reviews;
    }

    @JsonProperty(value="title")
    public String getTitle() {
        return title;
    }
}


