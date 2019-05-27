import java.io.*;

public class SQLFileReader {
    private static SQLFileReader ourInstance;

    public static SQLFileReader getInstance() {
        if (ourInstance == null)
            ourInstance = new SQLFileReader();
        return ourInstance;
    }

    private SQLFileReader() {
    }

    protected static void checkFileExistance(String statementFile) {

        String line = null;
        File file = new File(statementFile);

        if (!file.exists()) {
            try {
                throw new FileNotFoundException(statementFile + " does not exists.");
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            }
        }
    }

    protected static String fileToString(String statementFile) {
        StringBuffer stringBuffer = new StringBuffer();
        try {

            BufferedReader bufferedReader = new BufferedReader(new FileReader(statementFile));
            String line;
            line = bufferedReader.readLine();

            while (line != null) {
                if (!line.isEmpty())
                    stringBuffer.append(line).append("\n");
                line = bufferedReader.readLine();
            }

            bufferedReader.close();

        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException io) {
            io.printStackTrace();
        }finally {

        }

        return stringBuffer.toString();
    }


}


