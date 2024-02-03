import java.io.*;
import java.util.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;


class Pair implements WritableComparable<Pair> {
    public int i;
    public int j;
	
    Pair () {}
    Pair ( int i, int j ) { this.i = i; this.j = j; }

    /*...*/
}

public class Multiply extends Configured implements Tool {

    /* ... */

    @Override
    public int run ( String[] args ) throws Exception {
        /* ... */
        return 0;
    }

    public static void main ( String[] args ) throws Exception {
   	
    }
}
