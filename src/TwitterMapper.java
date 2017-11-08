
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.Calendar;
import java.util.Hashtable;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import java.util.*;

import org.apache.hadoop.io.NullWritable;
 import java.text.ParseException;
 import java.text.SimpleDateFormat;
 import org.apache.hadoop.io.DoubleWritable;

import org.apache.hadoop.mapreduce.Mapper;


public class TwitterMapper extends Mapper<Object, Text, Text, IntWritable> {


  	private Hashtable<String, String> companyInfo;

    private final IntWritable one = new IntWritable(1);
    private Text data = new Text();

    //1469453965000;757570957502394369;Over 30 million women footballers in the world. Most of us would trade places with this lot for #Rio2016  https://t.co/Mu5miVJAWx;<a href="http://twitter.com/download/iphone" rel="nofollow">Twitter for iPhone</a>

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

      try {


                String[] itr = value.toString().split(";"); // exploed and parsed to array and data type is string fix

                if(itr.length >= 4){ // only execute the complete array

                  Set<String> keys = companyInfo.keySet(); // create a set view of keys

                    for(String keyzz: keys){ // foreach each keys in hastable

                        if(itr[2].contains(keyzz)){ // if tweet contain key of hashtable , execute

                            String sports = companyInfo.get(keyzz); // get value of set key --> sports of athletes

                            data.set(sports); // write name of sport

                            context.write(data, one); // pass (k, v) to reducer


                        }

                    }

                }

        //end try
      } catch (NumberFormatException e) {
          System.err.println("NumberFormatException: " + e.getMessage());
      }//end catch

    } // end of map

    @Override
  	protected void setup(Context context) throws IOException, InterruptedException {

  		companyInfo = new Hashtable<String, String>();

  		// We know there is only one cache file, so we only retrieve that URI
  		URI fileUri = context.getCacheFiles()[0];

  		FileSystem fs = FileSystem.get(context.getConfiguration());
  		FSDataInputStream in = fs.open(new Path(fileUri));

  		BufferedReader br = new BufferedReader(new InputStreamReader(in));

  		String line = null;
  		try {
  			// we discard the header row
  			br.readLine();

  			while ((line = br.readLine()) != null) {

        	context.getCounter(CustomCounters.NUM_COMPANIES).increment(1);

  					// 736041664,A Jesus Garcia,ESP,male,10/17/69,1.72,64,athletics,0,0,0
            //id,name,nationality,sex,dob,height,weight,sport,gold,silver,bronze

  				String[] fields = line.split(",");
  				// Fields are: 0:Symbol 1:Name 2:IPOyear 3:Sector 4:industry
  				//TAKE ONLY NAME AND SPORTS
  				if (fields.length >= 5)
  					companyInfo.put(fields[1], fields[7]);
  														//keys    , // value
  			}
  			br.close();
  		} catch (IOException e1) {
  		}

  		super.setup(context);
  	} // end of hash

}// end of class
