import gudusoft.gsqlparser.EDbVendor;
import gudusoft.gsqlparser.TGSqlParser;
import gudusoft.gsqlparser.nodes.TTableList;
import gudusoft.gsqlparser.nodes.hive.THiveTablePartition;
import gudusoft.gsqlparser.stmt.TCreateTableSqlStatement;
import gudusoft.gsqlparser.stmt.TCreateViewSqlStatement;
import gudusoft.gsqlparser.stmt.TSelectSqlStatement;
import gudusoft.gsqlparser.stmt.hive.THiveSwitchDatabase;

import java.util.*;

//import junit.framework.TestCase;


public class HiveTableParsing {
    private static LinkedHashMap<String, ObjectClass> allObjectsMapList = new LinkedHashMap<>();
    private static LinkedHashMap<String, List<String>> stringListTreeMapTables = new LinkedHashMap<>();
    //private static Set<String> createTableSet = new HashSet<>();
    private static LinkedList<String> createTableSet = new LinkedList<>();
    private static String schemaName = "", qualifiedFullTableName = "", previousSchemaName = "";
    private static LinkedList<String> LineageCreateTableList = new LinkedList<>();
    private static LinkedList<String> NonLineageCreateTableList = new LinkedList<>();
    private static gudusoft.gsqlparser.TCustomSqlStatement statement;
    private static StringBuffer finalStatements = new StringBuffer();
    private static StringBuffer errorStatements = new StringBuffer();
    private static StringBuffer outStatements = new StringBuffer();
    private static Set<String> baseTablesReferedInViews = new HashSet<>();
    private static LinkedHashMap<String, Map<String, ObjectClass>> mapDatabases = new LinkedHashMap<>();
    private static LinkedHashMap<String, Integer> errorDetailMap = new LinkedHashMap<>();

    public static void main(String[] args) {
        //String statementFile;
        //statementFile = SQLFileReader.fileToString("src/main/resources/inputFiles/vwds_wcredit_ads_reference.ddl").replace("`", "");

        //List<String> statementsArray = new ArrayList<>();
        List<String> statementsArray;
        statementsArray = Arrays.asList(SQLFileReader.fileToString("src/test/resources/test.sql").replace("`", "").split(";"));
        //statementsArray = Arrays.asList(SQLFileReader.fileToString("src/main/resources/SampleSQL.txt").replace("`","").split(";"));

        TGSqlParser sqlparser = new TGSqlParser(EDbVendor.dbvhive);

        //Step 1: Gather all sql statements from a file.
        for (int y = 0; y < statementsArray.size(); y++)
            if (!statementsArray.get(y).isEmpty()) {
                try {
                    sqlparser.setSqltext(statementsArray.get(y));
                    sqlparser.parse();
                    if (sqlparser.getSqlstatements().size() != 0) mainX(sqlparser);

                } catch (Exception e) {
                    if (errorDetailMap.getOrDefault(schemaName, 0) == 0) errorDetailMap.put(schemaName, 1);

                    else errorDetailMap.put(schemaName, errorDetailMap.get(schemaName) + 1);

                    errorStatements.append(!schemaName.isEmpty() ? "use " + schemaName + ";\n" : statementsArray.get(y) + ";\n");
                    errorStatements.append(statementsArray.get(y)).append(";\n");
                    //e.printStackTrace();
                } finally {
                    // !schemaName.isEmpty() condition include because if NO "USE" statement is found in input file
                    //if (allObjectsMapList.size() != 0 && !schemaName.isEmpty())
                    //  mapDatabases.put(schemaName, allObjectsMapList); //Fix: If more than one USE DB; and it puts
                    // collected keys values if reached End of Array.
                }
            }
        //System.out.println(mapDatabases);
        //System.out.println(allObjectsMapList);
        //System.out.println(errorDetailMap);
        //System.out.println(errorStatements);

        sortInLineageOrder(stringListTreeMapTables.keySet());
        System.exit(1);
        //formFinalDDL();

        //SQLFileReader.writeIntoPlainFile("outputFiles/Error.sql", errorStatements);


    }

    public static void mainX(TGSqlParser sqlparser) {
        String createName;
        statement = sqlparser.sqlstatements.get(0);

        if (statement instanceof TCreateTableSqlStatement) {
            createName = findCreateTableSet((TCreateTableSqlStatement) statement);
            if (createTableSet.indexOf(createName) == -1)
                createTableSet.add(createName);

            allObjectsMapList.put(createName, new ObjectClass(schemaName,
                    ((TCreateTableSqlStatement) statement).getTableName().toString(), "TABLE",
                    statement.toString()));

        } else if (statement instanceof TCreateViewSqlStatement) {
            if (((TCreateViewSqlStatement) statement).getViewName().getSchemaString().isEmpty() & !schemaName.isEmpty())
                createName = new StringBuilder().append(schemaName).append(".").append(((TCreateViewSqlStatement) statement).getViewName()).toString();
            else
                createName = ((TCreateViewSqlStatement) statement).getViewName().toString();
            baseTablesReferedInViews = new HashSet<>();
            //  While Gathering View statements, gather its' parent tables/views
            findBaseTableFromView(((TCreateViewSqlStatement) statement).getSubquery());
            stringListTreeMapTables.put(createName.toLowerCase(), new LinkedList<>(baseTablesReferedInViews));

            allObjectsMapList.put(createName, new ObjectClass(schemaName, ((TCreateViewSqlStatement) statement).getViewName().toString(), "VIEW", statement.toString()));

        } else if (statement instanceof THiveSwitchDatabase) {
            THiveSwitchDatabase tHiveSwitchDatabase = (THiveSwitchDatabase) statement;
            schemaName = tHiveSwitchDatabase.getDbName().getPartString().toLowerCase();
            if (!previousSchemaName.isEmpty()) {
                mapDatabases.put(previousSchemaName, allObjectsMapList);
                allObjectsMapList = new LinkedHashMap<>();
            }
            previousSchemaName = schemaName;
        }
    }

    public static void sortInLineageOrder(Set createObjectNames) {
        System.out.println("stringListTreeMapTables->" + stringListTreeMapTables + "\n");
        NonLineageCreateTableList.addAll(createObjectNames);
        System.out.println("NonLineageCreateTableList::" + NonLineageCreateTableList + "\n");
        for (String Table : NonLineageCreateTableList) {
            findLineage(Table, stringListTreeMapTables);
        }
        System.out.println("LineageCreateTableList Size:" + LineageCreateTableList.size() + "::: " + LineageCreateTableList.toString() + "\n");

    }

    //public static void findBaseTableFromView(TSelectSqlStatement viewStatement) throws JSQLParserException {
    public static void findBaseTableFromView(TSelectSqlStatement selectStatement) { //throws JSQLParserException {
        //TSelectSqlStatement stat = (TSelectSqlStatement) viewStatement.getSubquery();
        //List aa = Arrays.asList(selectStatement.getTables());
        TTableList tTableList = selectStatement.getTables();

        int i = tTableList.size() - 1;
        while (i >= 0) {
            //String tt1 = tTableList.getTable(i).toString();
            //Boolean bb1 = tTableList.getTable(i).isBaseTable();
            if (tTableList.getTable(i).isBaseTable()) {
                //String tmp1 = tTableList.getTable(i).getPrefixSchema().toString();
                //System.out.println("tmp1:" + tmp1);
                //if (tTableList.getTable(i).getPrefixSchema() == null & !schemaName.isEmpty())
                if (tTableList.getTable(i).getPrefixSchema().isEmpty() & !schemaName.isEmpty())
                    qualifiedFullTableName = schemaName + "." + tTableList.getTable(i);
                else
                    qualifiedFullTableName = tTableList.getTable(i).toString();

                // System.out.println("1-qualifiedFullTableName:" + qualifiedFullTableName);
                //System.out.println("1-baseTablesReferedInViews:" + baseTablesReferedInViews);
                //if (baseTablesReferedInViews.indexOf(qualifiedFullTableName) == -1)
                if (!baseTablesReferedInViews.contains(qualifiedFullTableName))
                    baseTablesReferedInViews.add(qualifiedFullTableName.toLowerCase());

                // System.out.println("2-baseTablesReferedInViews:" + baseTablesReferedInViews);
            } else {
                findBaseTableFromView(tTableList.getTable(i).getSubquery());
            }

            i--;
        }

        //return baseTablesReferedInViews;
    }

    public static String findCreateTableSet(TCreateTableSqlStatement tableStatement) {
        //System.out.println("schemaName:" + schemaName);
        //findPartitionColumns(tableStatement);
        //if (tableStatement.getTableName().getSchemaString() == null & !schemaName.isEmpty())
        if (tableStatement.getTableName().getSchemaString().isEmpty() & !schemaName.isEmpty())
            qualifiedFullTableName = schemaName + "." + tableStatement.getTableName().getTableString();
        else
            qualifiedFullTableName = tableStatement.getTableName().toString();

        return qualifiedFullTableName.toLowerCase();
    }

    public static void findPartitionColumns(TCreateTableSqlStatement tableStatement) {
        THiveTablePartition tp = tableStatement.getHiveTablePartition();
        //assertTrue(tp.getColumnDefList().size() == 2);
        //System.out.println("tp.getColumnDefList().size():" + tp.getColumnDefList().size());

        List a = Arrays.asList(tableStatement.getHiveTablePartition().getColumnDefList().toString());
        for (Object s : a) {

            //assertTrue(tp.getColumnDefList().getColumn(0).getColumnName().toString().equalsIgnoreCase("dt"));
            //assertTrue(tp.getColumnDefList().getColumn(1).getDatatype().getDataType() == EDataType.string_t);
            //for (String s : tp.getColumnDefList().getC){
                /*for (int i = 0; i < tp.getColumnDefList().size(); i++) {
                    System.out.println("PARTITION:" + tp.getColumnDefList().getColumn(i).getColumnName());
                }*/
        }

    }

    /**
     * Find the Lineage of a View
     *
     * @param Table
     * @param tablesMap
     */
    public static void findLineage(String Table, Map<String, List<String>> tablesMap) {
        /**
         * Checks first whether the child base table is also has a CREATE TABLE statement
         */
        if ((NonLineageCreateTableList.indexOf(Table)) != -1) {
            List<String> baseTableList = tablesMap.get(Table);

            for (String baseTable : baseTableList) {
                if (Table.equalsIgnoreCase(baseTable)) System.out.println("DEADLOCK::" + baseTable);
                else
                    findLineage(baseTable, tablesMap);
            }
            if (LineageCreateTableList.indexOf(Table) == -1) LineageCreateTableList.add(Table);
        }
    }

    public void formFinalDDL() {
        /**
         * Writing into int new SQL file.
         */
        /**
         * Logic for DROP Statements in linage order
         */
        LinkedList<String> newFinalCreateObjects = new LinkedList<>();
        ObjectClass oc;
        Collections.sort(createTableSet);
        for (String ct : createTableSet) {
            oc = allObjectsMapList.get(ct);

            if (newFinalCreateObjects.indexOf("USE " + oc.objDB + ";") == -1 && !oc.objDB.isEmpty())
                newFinalCreateObjects.add("USE " + oc.objDB + ";");
            switch (oc.objType) {
                case "TABLE":
                    newFinalCreateObjects.add("DROP TABLE IF EXISTS " + ct + ";");
                    break;
                case "VIEW":
                    newFinalCreateObjects.add("DROP VIEW IF EXISTS " + ct + ";");
                    break;
            }
        }


        newFinalCreateObjects.add(newFinalCreateObjects.indexOf("USE test1;"), "I LOVE YOU");
        System.out.println(newFinalCreateObjects);

        /*oc = allObjectsMapList.get(createTablename);
        GFmtOpt option = GFmtOptFactory.newInstance();
        sqlparser.setSqltext(oc.objDdl);
        sqlparser.parse();*/

        //newFinalCreateObjects.add(FormatterFactory.pp(sqlparser, option) + ";\n");

        SQLFileReader.AppendToFile("src/test/resources/outputFiles", newFinalCreateObjects);
        //System.out.println(newFinalCreateObjects);
    }
}


class ObjectClass {
    String objDB, objName, objType, objDdl;

    ObjectClass(String ObjectDB, String ObjectName, String ObjectType, String ObjectDdl) {
        this.objDB = ObjectDB;
        this.objName = ObjectName;
        this.objType = ObjectType;
        this.objDdl = ObjectDdl;
    }

    @Override
    public String toString() {
        return "ObjectClass{" +
                "objDB='" + objDB + '\'' +
                "objName='" + objName + '\'' +
                ", objType='" + objType + '\'' +
                ", objDdl='" + objDdl + '\'' +
                '}';
    }

    public String getObjName() {
        return objName;
    }

    public void setObjName(String objName) {
        this.objName = objName;
    }

    public String getObjType() {
        return objType;
    }

    public void setObjType(String objType) {
        this.objType = objType;
    }

    public String getObjDdl() {
        return objDdl;
    }

    public void setObjDdl(String objDdl) {
        this.objDdl = objDdl;
    }
}