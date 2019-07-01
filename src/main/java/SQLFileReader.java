import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.LinkedList;

public class SQLFileReader {
    private static SQLFileReader ourInstance;
    //public static final String digestDirectory = FileUtils.getTempDirectoryPath() + "cdc_";

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
            line = bufferedReader.readLine().trim();

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
        } finally {

        }

        return stringBuffer.toString();
    }

    public static void writeIntoPlainFile(String fileName, StringBuffer stringBuffer) {

        BufferedWriter bufferedWriter;
        Path path;
        File file;
        //Path path = null;
        try {
            file = new File(fileName);

            if (stringBuffer.length() != 0) {
                if (!file.exists())
                    file.getParentFile().mkdir();//System.out.println("PARENT:" +file.getParentFile());//.mkdir();
                else file.delete();
            }
            bufferedWriter = new BufferedWriter(new FileWriter(file));
            bufferedWriter.write(stringBuffer.toString());
            bufferedWriter.flush();
            bufferedWriter.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void AppendToFile(String fileDir, LinkedList<String> Content) {
        File processDirectory;
        try {
            processDirectory = new File(fileDir);
            String fileName = processDirectory + "\\" + "new" + ".sql";
            //System.out.println("fileName->" + fileName);
            //else if (murInd == 0) fileName=".txt";
            //final Path path = Paths.get(processDirectory.getAbsolutePath(), fileName);
            final Path path = Paths.get(fileName);
            if (processDirectory.exists()) {

                //final Path path = Paths.get(fileName);
                //Files.write(path, Arrays.asList(murInd + "\t" + data), StandardCharsets.UTF_8, Files.exists(path) ? StandardOpenOption.APPEND : StandardOpenOption.CREATE);
                Files.write(path, Content, StandardCharsets.UTF_8, Files.exists(path) ? StandardOpenOption.APPEND : StandardOpenOption.CREATE);
            } else {
                processDirectory.mkdir();
                Files.write(path, Content, StandardCharsets.UTF_8, Files.exists(path) ? StandardOpenOption.APPEND : StandardOpenOption.CREATE);
            }

        } catch (IOException ioe) {
            System.out.println("");
            ioe.printStackTrace();
        }
    }

}