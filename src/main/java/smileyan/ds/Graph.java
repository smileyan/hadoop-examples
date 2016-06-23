package smileyan.ds;

import java.util.Stack;

/**
 * Created by hua on 23/06/16.
 */
public abstract class Graph<Tv, Te> {

    // vertex
    private int n;

    abstract public int insert(Tv v);
    abstract public Tv remove(int i);
    abstract public Tv vertex(int i);
    abstract public int inDegree(int i);
    abstract public int outDegree(int i);
    abstract public int firstNbr(int i);
    abstract public int nextNbr(int i, int j);
    abstract public VStatus status(int i, VStatus status);
    abstract public int dTime(int i, int time);
    abstract public int fTime(int i, int time);
    abstract public int parent(int i, int j);
    abstract public int priority(int i, int n);


    // edge
    private int e;

    abstract boolean exists(int i, int j);
    abstract void insert(Te e, int i, int j, int weight);
    abstract Te remove(int i, int j);
    abstract EType type(int i, int j, EType type);
    abstract Te edge(int i, int j);
    abstract int weight(int i, int j);


    // alg
    abstract void bfs (int i);
    abstract void dfs (int i);
    abstract void bcc (int i);
    abstract Stack<Tv> tSort (int i);
    abstract void prim (int i);
    abstract void dijkstra (int i);
    // abstract void pfs<PU> (int i, PU pu);

    
    private void reset() {
        for (int i = 0; i < n; i++) {
            status(i, VStatus.UNDISCOVERED); dTime(i, -1); fTime(i, -1);
            parent(i, -1); priority(i, Integer.MAX_VALUE);
            for (int j = 0; i < n; j++) {
                if (exists(i, j)) {
                    type(i, j, EType.UNDETERMINED);
                }
            }
        }
    }
}
