public class DDLRefresh {

    public static void main(String[] args) {
        SQLFileReader sqlFileReader = SQLFileReader.getInstance();
        String sqlFile = "D:\\Users\\Rajesh\\Desktop\\SampleSQL.txt";
        sqlFileReader.checkFileExistance(sqlFile);

        StatementParser statementParser = new StatementParser();
        statementParser.readFile(sqlFile);


    }


}
