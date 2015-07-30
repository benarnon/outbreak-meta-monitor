package createVector;


/**
 * Created by user on 3/2/15.
 */
public class ResTuple {
    private String pathogen;
    private String score;




    public String getPathogen() {
        return pathogen;
    }

    public void setPathogen(String pathogen) {
        this.pathogen = pathogen;
    }

    public String getScore() {
        return score;
    }

    public void setScore(String score) {
        this.score = score;
    }

    public ResTuple(String pathogen, String score) {
        this.pathogen = pathogen;
        this.score = score;


    }

    @Override
    public String toString() {
        return pathogen +"\t" + score;
    }
}
