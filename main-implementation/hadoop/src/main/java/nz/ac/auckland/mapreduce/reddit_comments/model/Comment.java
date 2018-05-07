package nz.ac.auckland.mapreduce.reddit_comments.model;

public class Comment {

    private final int ups;

    private final String subReddit;

    public Comment(int ups, String subReddit) {
        this.ups = ups;
        this.subReddit = subReddit;
    }

    public int getUps() {
        return ups;
    }

    public String getSubReddit() {
        return subReddit;
    }

    @Override
    public String toString() {
        return "Comment{" +
                "ups=" + ups +
                ", subReddit='" + subReddit + '\'' +
                '}';
    }
}
