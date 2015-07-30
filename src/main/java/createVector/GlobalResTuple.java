package createVector;


/**
 * Created by user on 3/5/15.
 */
public class GlobalResTuple {
    private String pathogen;
    private float score;
    private String ClusterID;

    public GlobalResTuple(String pathogen, float score, String clusterID) {
        this.pathogen = pathogen;
        this.score = score;
        ClusterID = clusterID;
    }

    public String getPathogen() {
        return pathogen;
    }

    public void setPathogen(String pathogen) {
        this.pathogen = pathogen;
    }

    public String getClusterID() {
        return ClusterID;
    }

    public void setClusterID(String clusterID) {
        ClusterID = clusterID;
    }

    public float getScore() {
        return score;
    }

    public void setScore(float score) {
        this.score = score;
    }


}
