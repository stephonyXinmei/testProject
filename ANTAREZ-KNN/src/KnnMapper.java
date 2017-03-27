import org.apache.commons.io.FileUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.File;
import java.io.IOException;
import java.util.*;

/**
 * Created by xm on 27/03/2017.
 */
public class KnnMapper extends Mapper<Object, Text, NullWritable, DistanceQuery> {

    private static final int VEC_DIMENSION= 300;

    private static final int K = 100;

    private static DistanceQuery distanceAndQuery = new DistanceQuery();
    private static TreeMap<Double, String> knnMap = new TreeMap<Double, String>(new Comparator<Double>() {
        @Override
        public int compare(Double o1, Double o2) {
            return o2.compareTo(o1);
        }
    });
    private static Vector<Double> traceFeatureVec = new Vector<Double>(VEC_DIMENSION);

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        if (context.getCacheFiles() != null && context.getCacheFiles().length > 0) {
            String knnParams = FileUtils.readFileToString(new File("./knnParamFile"));
            String[] vecList = knnParams.split(",");
            for (String x : vecList) {
                traceFeatureVec.addElement(Double.valueOf(x));
            }
        }
    }

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] cols = line.split("\t");

        String query = cols[0];
        String[] vecList = cols[1].split(",");

        Vector<Double> featureVec = new Vector<Double>(VEC_DIMENSION);

        for (String x : vecList) {
            featureVec.addElement(Double.valueOf(x));
        }

        Double dist = FeatureSimilarity.getCosineSimilarity(traceFeatureVec, featureVec);

        if (dist > 0.0) {
            knnMap.put(dist, query);

            if (knnMap.size() > K) {
                knnMap.remove(knnMap.lastKey());
            }
        }
    }

    @Override
    // The cleanup() method is run once after map() has run for every row
    protected void cleanup(Context context) throws IOException, InterruptedException {
        for(Map.Entry<Double, String> entry : knnMap.entrySet()) {
            Double knnDist = entry.getKey();
            String knnQuery = entry.getValue();
            distanceAndQuery.set(knnDist, knnQuery);
            context.write(NullWritable.get(), distanceAndQuery);
        }
    }
}
