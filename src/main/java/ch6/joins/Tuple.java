package ch6.joins;

import org.apache.hadoop.io.Text;

/**
 * Created by hua on 05/08/16.
 */
public class Tuple{
    int type;
    String value;
    Text text;

    public void setInt(int type) {
        this.type = type;
    }

    public int getInt() {
        String tmp = text.toString();
        return Integer.parseInt(tmp.substring(tmp.length() - 2));
    }

    public void setString(String value) {
        this.value = value;
    }

    public String getString() {
        String tmp = text.toString();
        return tmp.substring(0, tmp.length() - 3);
    }

    public void setText(Text text) {
        this.text = text;
    }

    public Text toText() {
        return new Text(value + "\t" + type);
    }
}
