import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class StatementParser {

    private List<String> dbStatments = new ArrayList<>();

    public List<String> getDbStatments() {
        return dbStatments;
    }

    public void setDbStatments(List<String> dbStatments) {
        this.dbStatments = dbStatments;
    }

    public static void addStatement() {

    }

    protected void readFile(String statementFile) {
        try {

            String line = null;
            BufferedReader bufferedReader = new BufferedReader(new FileReader(new File(statementFile)));
            while ((line = bufferedReader.readLine()) != null) {
                System.out.println(line);
            }

        } catch (IOException io) {
            System.out.println("Unable to read file:" + statementFile);
            io.printStackTrace();
        }
    }
}
