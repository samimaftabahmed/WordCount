import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

public class Map extends Mapper<LongWritable, Text, Text, IntWritable> {

    private final static IntWritable IN_WRITABLE_ONE = new IntWritable(1);
    private String regex = "[~`!^&*()_+=/\\-,?.;:'\"]";
    private Text word = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        StringTokenizer stringTokenizer = new StringTokenizer(value.toString());

        while (stringTokenizer.hasMoreElements()) {

            String token = stringTokenizer.nextToken().trim();
            int tokenLength = token.length();

            if (tokenLength > 0 && String.valueOf(token.charAt(0)).matches(regex)) {
                token = token.substring(1);
                tokenLength = token.length();
            }

            if (tokenLength > 2) {
                this.dataCleansingTwo(token, tokenLength, context);

            } else if (tokenLength > 1) {
                this.dataCleansingOne(token, tokenLength, context);
            }
        }
    }

    private void dataCleansingTwo(String token, int tokenLength, Context context) {

        boolean lastCharSecondStat = checkCharacter(token.charAt(tokenLength - 2));
        boolean lastCharStat = checkCharacter(token.charAt(tokenLength - 1));

        if (lastCharStat && lastCharSecondStat) {

            String newToken = token.substring(0, tokenLength - 2).trim();
            this.outData(newToken, context);

        } else if (lastCharSecondStat) {

            String[] split = token.split(regex);

            for (String s : split) {
                this.outData(s.trim(), context);
            }

        } else if (lastCharStat) {

            String newToken = token.substring(0, tokenLength - 1).trim();
            this.outData(newToken, context);

        } else {
            this.outData(token, context);
        }
    }

    private void dataCleansingOne(String token, int tokenLength, Context context) {

        boolean lastCharStat = checkCharacter(token.charAt(tokenLength - 1));

        if (lastCharStat) {

            String newToken = token.substring(0, tokenLength - 1).trim();
            this.outData(newToken, context);
        }
    }

    private boolean checkCharacter(char c) {

        return String.valueOf(c).matches(regex);
    }

    private void outData(String data, Context context) {

        try {
            word.set(data);
            context.write(word, IN_WRITABLE_ONE);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
