package nz.ac.auckland.mapreduce.reddit_comments.model;

public class Comment {

    private final int votes;

    private final String subReddit;

    public Comment(int votes, String subReddit) {
        this.votes = votes;
        this.subReddit = subReddit;
    }

    public int getVotes() {
        return votes;
    }

    public String getSubReddit() {
        return subReddit;
    }

    @Override
    public String toString() {
        return "Comment{" +
                "votes=" + votes +
                ", subReddit='" + subReddit + '\'' +
                '}';
    }
}
