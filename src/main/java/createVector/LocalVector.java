package createVector;


import java.util.List;

/**
 * Created by user on 3/2/15.
 */
public class LocalVector implements java.io.Serializable {
    private String CluserID;
    private List<ResTuple> ResVector;

    @Override
    public String toString() {
        String ans ="";
        for (ResTuple res:ResVector){
            ans = ans + CluserID + "\\" + res.getPathogen() + "\t" + res.getScore() + "\n" ;

        }


        return ans;
    }

    public LocalVector(String cluserID, List<ResTuple> ResVector) {
        this.CluserID = cluserID;
        this.ResVector = ResVector;
    }


    public String getCluserID() {
        return CluserID;
    }

    public void setCluserID(String cluserID) {
        CluserID = cluserID;
    }

    public void AddResTuple(ResTuple res){
        ResVector.add(res);
    }

    public ResTuple GetResTuple(int i){
        return ResVector.get(i);
    }

    public int GetSizeResVector(){
        return ResVector.size();
    }

}