public class TestHiveDdl {
    public static void main(String[] args) {
        HiveTableParsing htp = new HiveTableParsing();
        //htp.mainX("src/main/resources/inputFiles/vwds_wcredit_ads_reference.ddl");
        htp.mainX("src/main/resources/SampleSQL.sql");
        //htp.mainX("src/test/resources/test.sql");

    }
}
