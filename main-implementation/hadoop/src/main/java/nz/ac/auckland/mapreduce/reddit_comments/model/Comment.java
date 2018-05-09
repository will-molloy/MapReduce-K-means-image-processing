package nz.ac.auckland.mapreduce.reddit_comments.model;

public class Comment {

    private final int score;

    private final String subReddit;

    private final int gilded;

    public Comment(int score, String subReddit, int gilded) {
        this.score = score;
        this.subReddit = subReddit.toLowerCase();
        this.gilded = gilded;
    }

    public int getScore() {
        return score;
    }

    public String getSubReddit() {
        return subReddit;
    }

    public boolean isGilded() {
        return gilded > 0;
    }

}
