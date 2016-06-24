package smileyan.ds;

/**
 * Created by hua on 24/06/16.
 */
public class Vertex<Tv> {
    Tv data; int inDegree, outDegree; VStatus status;
    int dTime, fTime;
    int parent; int priority;

    public Tv getData() {
        return data;
    }

    public int getInDegree() {
        return inDegree;
    }

    public int getOutDegree() {
        return outDegree;
    }

    public VStatus getStatus() {
        return status;
    }

    public int getdTime() {
        return dTime;
    }

    public int getfTime() {
        return fTime;
    }

    public int getParent() {
        return parent;
    }

    public int getPriority() {
        return priority;
    }

    public void setData(Tv data) {
        this.data = data;
    }

    public void setInDegree(int inDegree) {
        this.inDegree = inDegree;
    }

    public void setOutDegree(int outDegree) {
        this.outDegree = outDegree;
    }

    public void setStatus(VStatus status) {
        this.status = status;
    }

    public void setdTime(int dTime) {
        this.dTime = dTime;
    }

    public void setfTime(int fTime) {
        this.fTime = fTime;
    }

    public void setParent(int parent) {
        this.parent = parent;
    }

    public void setPriority(int priority) {
        this.priority = priority;
    }

    public Vertex(Tv data) {
        setData(data);
        setInDegree(0);
        setOutDegree(0);
        setStatus(VStatus.UNDISCOVERED);
        setdTime(-1);
        setfTime(-1);
        setParent(-1);
        setPriority(Integer.MAX_VALUE);
    }
}
