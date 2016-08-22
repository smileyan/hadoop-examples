package ch6.sort.secondary;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by hua on 19/08/16.
 */
public class Person implements WritableComparable<Person> {


    private String firstName;



    private String lastName;

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.firstName = dataInput.readUTF();
        this.lastName = dataInput.readUTF();
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(firstName);
        dataOutput.writeUTF(lastName);
    }

    @Override
    public int compareTo(Person o) {

        int result = firstName.compareTo(o.firstName);

        if ( result != 0 ) {
            return result;
        }

        return lastName.compareTo(o.lastName);
    }


    public String getLastName() {
        return lastName;
    }

    public void setLastName(String lastName) {
        this.lastName = lastName;
    }
    public String getFirstName() {
        return firstName;
    }

    public void setFirstName(String firstName) {
        this.firstName = firstName;
    }
}
