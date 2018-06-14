import com.ibeifeng.senior.usertrack.spark.session.UserVisitActionAnalyzerSpark;


public class UserVisitActionAnalyzerSparkDemo {
    public static void main(String[] args) throws InterruptedException {
        String[] params = new String[]{"1"};
        UserVisitActionAnalyzerSpark.main(params);
        Thread.sleep(10000000);
    }
}
