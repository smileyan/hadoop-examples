package ch6.sort.secondary;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * Created by hua on 23/08/16.
 */
public class PersonComparator extends WritableComparator{

    protected PersonComparator() {
        super(Person.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {

        Person p1 = (Person) a;
        Person p2 = (Person) b;

        int cmp = p1.getLastName().compareTo(p2.getLastName());
        if ( cmp != 0 ) {
            return cmp;
        }

        return p1.getFirstName().compareTo(p2.getFirstName());

    }
}
