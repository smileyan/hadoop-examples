package smileyan.ds;

/**
 * Created by hua on 24/06/16.
 */
public class Edge<Te> {
    Te data;
    int weight;
    EType type;

    public Edge(Te data, int weight) {
        setData(data);
        setWeight(weight);
        setType(EType.UNDETERMINED);
    }

    public Te getData() {
        return data;
    }

    public int getWeight() {
        return weight;
    }

    public EType getType() {
        return type;
    }

    public void setData(Te data) {
        this.data = data;
    }

    public void setWeight(int weight) {
        this.weight = weight;
    }

    public void setType(EType type) {
        this.type = type;
    }
}
