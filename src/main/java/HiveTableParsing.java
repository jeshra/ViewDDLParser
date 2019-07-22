import gudusoft.gsqlparser.EDbVendor;
import gudusoft.gsqlparser.TGSqlParser;
import gudusoft.gsqlparser.nodes.TObjectName;
import gudusoft.gsqlparser.nodes.TTable;
import gudusoft.gsqlparser.nodes.TTableList;
import gudusoft.gsqlparser.nodes.hive.THiveTablePartition;
import gudusoft.gsqlparser.stmt.TCreateTableSqlStatement;
import gudusoft.gsqlparser.stmt.TCreateViewSqlStatement;
import gudusoft.gsqlparser.stmt.TSelectSqlStatement;
import gudusoft.gsqlparser.stmt.hive.THiveSwitchDatabase;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

//import junit.framework.TestCase;


public class HiveTableParsing implements HiveDdl {
    private final String lanePattern = "(.*)(_)(tl|uat|dev|sit)(\\d+)";
    private final Pattern pattern = Pattern.compile(lanePattern, Pattern.CASE_INSENSITIVE);
    private LinkedHashMap<String, List<String>> stringListTreeMapTables = new LinkedHashMap<>();
    //private  Set<String> createTableSet = new HashSet<>();
    private LinkedList<String> createTableSet = new LinkedList<>();
    private String schemaName = "", qualifiedFullTableName = "", previousSchemaName = "";
    private LinkedList<String> LineageCreateTableList = new LinkedList<>();
    private LinkedList<String> NonLineageCreateTableList = new LinkedList<>();
    private gudusoft.gsqlparser.TCustomSqlStatement statement;
    private StringBuffer finalStatements = new StringBuffer();
    private StringBuffer errorStatements = new StringBuffer();
    private StringBuffer outStatements = new StringBuffer();
    private Set<String> baseTablesReferedInViews = new HashSet<>();
    private LinkedHashMap<String, Map<String, ObjectClass>> mapDatabases = new LinkedHashMap<>();
    private LinkedHashMap<String, Integer> errorDetailMap = new LinkedHashMap<>();
    private LinkedHashMap<String, ObjectClass> allObjectsMapList1 = new LinkedHashMap<>();
    private LinkedHashMap<String, LinkedList<String>> stringLinkedListLinkedHashMap = new LinkedHashMap<>();

    //public void mainX(String[] args) {
    public void mainX(String fileName) {
        List<String> statementsArray;
        statementsArray = Arrays.asList(SQLFileReader.fileToString(fileName).replace("`", "").split(";"));

        TGSqlParser sqlparser = new TGSqlParser(EDbVendor.dbvhive);

        //Step 1: Gather all sql statements from a file.
        try {
            for (int y = 0; y < statementsArray.size(); y++)
                if (!statementsArray.get(y).isEmpty()) {
                    try {
                        sqlparser.setSqltext(statementsArray.get(y));
                        sqlparser.parse();
                        if (sqlparser.getSqlstatements().size() != 0) mainY(sqlparser);

                    } catch (Exception e) {
                        if (errorDetailMap.getOrDefault(schemaName, 0) == 0) errorDetailMap.put(schemaName, 1);

                        else errorDetailMap.put(schemaName, errorDetailMap.get(schemaName) + 1);

                        errorStatements.append(!schemaName.isEmpty() ? "use " + schemaName + ";\n" : statementsArray.get(y) + ";\n");
                        errorStatements.append(statementsArray.get(y)).append(";\n");
                        e.printStackTrace();
                    }
                }
        } catch (Exception e) {

        } finally {
            /*//<editor-fold desc="!schemaName.isEmpty() condition include because if NO 'USE' statement is found in input file">
            //</editor-fold>
            if (allObjectsMapList1.size() != 0 && !schemaName.isEmpty()) {
                mapDatabases.put(schemaName, allObjectsMapList1); //Fix: If more than one USE DB; and it puts
            }*/
        }
        {
            //System.out.println(mapDatabases);
            System.out.println("allObjectsMapList1:" + allObjectsMapList1);
            //System.out.println(errorDetailMap);
            //System.out.println(errorStatements);
        }
        sortInLineageOrder(stringListTreeMapTables.keySet());
        System.out.println("createTableSet->" + createTableSet);
        System.out.println("LineageCreateTableList Size:" + LineageCreateTableList.size() + "::: " + LineageCreateTableList.toString() + "\n");


        //<editor-fold desc="You can have controls with flags to process only Tables, Views, Synonmys ">
        stringLinkedListLinkedHashMap = linageListToMap(createTableSet);
        //</editor-fold>
        stringLinkedListLinkedHashMap = linageListToMap(LineageCreateTableList);
        System.out.println("stringLinkedListLinkedHashMap:" + stringLinkedListLinkedHashMap + "\n");
        //System.out.println(mapDatabases);

        //region Description
        formFinalDDL(stringLinkedListLinkedHashMap);
        //endregion

        //SQLFileReader.writeIntoPlainFile("outputFiles/Error.sql", errorStatements);
    }

    public void mainY(TGSqlParser sqlparser) {
        String createName;
        statement = sqlparser.sqlstatements.get(0);
        TObjectName o;

        if (statement instanceof TCreateTableSqlStatement) {
            TCreateTableSqlStatement cTable =
                    ((TCreateTableSqlStatement) statement);
            createName = findCreateTableSet(cTable);

            if (createTableSet.indexOf(createName) == -1) createTableSet.add(createName);

            TSelectSqlStatement select = (TSelectSqlStatement) cTable.getSubQuery();
            System.out.println("select = " + select);


            TTable t;
            for (int i = 0; i < select.tables.size(); i++) {
                t = select.tables.getTable(i);
                System.out.println("t = " + t);

                convertLane(t, "tl3");
                /*Matcher m = pattern.matcher(t.toString());

                if (m.find()) {
                    System.err.println("Already suffix <" + m.group(2) + m.group(3) + m.group(4) + "> found in " +
                            "tablename " + t);
                } else {
                    t.getTableName().getObjectToken().astext = t + "_tl3";
                }*/
            }
            System.out.println("cTable = " + cTable);
            System.exit(1);
            allObjectsMapList1.put(createName, new ObjectClass(schemaName,
                    cTable.getTableName().toString(), "TABLE",
                    statement.toString()));

        } else if (statement instanceof TCreateViewSqlStatement) {
            TCreateViewSqlStatement cView = ((TCreateViewSqlStatement) statement);
            if (cView.getViewName().getSchemaString().isEmpty() & !schemaName.isEmpty())
                createName = new StringBuilder().append(schemaName).append(".").append(cView.getViewName()).toString();
            else createName = cView.getViewName().toString();

            baseTablesReferedInViews = new HashSet<>();
            //  While Gathering View statements, gather its' parent tables/views
            findBaseTableFromView(cView.getSubquery());
            stringListTreeMapTables.put(createName.toLowerCase(), new LinkedList<>(baseTablesReferedInViews));

            allObjectsMapList1.put(createName, new ObjectClass(schemaName, cView.getViewName().toString(), "VIEW", statement.toString()));

        } else if (statement instanceof THiveSwitchDatabase) {
            THiveSwitchDatabase tHiveSwitchDatabase = (THiveSwitchDatabase) statement;
            schemaName = tHiveSwitchDatabase.getDbName().getPartString().toLowerCase();
            if (!previousSchemaName.isEmpty() && !schemaName.equalsIgnoreCase(previousSchemaName)) {
                mapDatabases.put(previousSchemaName, allObjectsMapList1);
            }
            /*previousSchemaName = schemaName;
            if (mapDatabases.get(schemaName) == null)
                allObjectsMapList1 = new LinkedHashMap<>();
            else {
                allObjectsMapList1 = new LinkedHashMap<>(mapDatabases.get(schemaName));
            }*/
        }
    }

    public void sortInLineageOrder(Set createObjectNames) {
        System.out.println("stringListTreeMapTables->" + stringListTreeMapTables + "\n");
        NonLineageCreateTableList.addAll(createObjectNames);
        System.out.println("NonLineageCreateTableList::" + NonLineageCreateTableList + "\n");
        for (String Table : NonLineageCreateTableList) {
            findLineage(Table, stringListTreeMapTables);
        }

    }

    public void findBaseTableFromView(TSelectSqlStatement selectStatement) { //throws JSQLParserException {
        TTableList tTableList = selectStatement.getTables();
        int i = tTableList.size() - 1;
        while (i >= 0) {
            if (tTableList.getTable(i).isBaseTable()) {
                //String tmp1 = tTableList.getTable(i).getPrefixSchema().toString();
                //System.out.println("tmp1:" + tmp1);
                //if (tTableList.getTable(i).getPrefixSchema() == null & !schemaName.isEmpty())
                if (tTableList.getTable(i).getPrefixSchema().isEmpty() & !schemaName.isEmpty())
                    qualifiedFullTableName = schemaName + "." + tTableList.getTable(i);
                else
                    qualifiedFullTableName = tTableList.getTable(i).toString();
                if (!baseTablesReferedInViews.contains(qualifiedFullTableName))
                    baseTablesReferedInViews.add(qualifiedFullTableName.toLowerCase());
            } else {
                findBaseTableFromView(tTableList.getTable(i).getSubquery());
            }
            i--;
        }
        //return baseTablesReferedInViews;
    }

    public String findCreateTableSet(TCreateTableSqlStatement tableStatement) {
        //findPartitionColumns(tableStatement);
        System.out.println("tableStatement.getTableName() = " + tableStatement.getTableKinds());
        if (tableStatement.getTableName().getSchemaString().isEmpty() & !schemaName.isEmpty())
            qualifiedFullTableName = schemaName + "." + tableStatement.getTableName().getTableString();
        else
            qualifiedFullTableName = tableStatement.getTableName().toString();

        return qualifiedFullTableName.toLowerCase();
    }

    public void findPartitionColumns(TCreateTableSqlStatement tableStatement) {
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
    public void findLineage(String Table, Map<String, List<String>> tablesMap) {
        /**
         * Checks first whether the child base table is also has a CREATE TABLE statement
         */
        if ((NonLineageCreateTableList.indexOf(Table)) != -1) {
            List<String> baseTableList = tablesMap.get(Table);

            for (String baseTable : baseTableList) {
                if (Table.equalsIgnoreCase(baseTable)) System.err.println("DEADLOCK::" + baseTable);
                else
                    findLineage(baseTable, tablesMap);
            }
            if (LineageCreateTableList.indexOf(Table) == -1) LineageCreateTableList.add(Table);
        }
    }

    public void formFinalDDL(LinkedHashMap stringLinkedList) {
        LinkedList<String> newFinalCreateObjects = new LinkedList<>();
        ObjectClass oc;
        //for (Map.Entry<String, Map<String, ObjectClass>> entry : mapDatabases.entrySet()) {
        for (Map.Entry<String, LinkedList<String>> entry : stringLinkedListLinkedHashMap.entrySet()) {
            //if (newFinalCreateObjects.indexOf("USE " + entry.getKey() + ";") == -1)
            newFinalCreateObjects.add("USE " + entry.getKey() + ";\n");

            //Process each elements in LinkedList
            for (String o : entry.getValue()) {
                oc = allObjectsMapList1.get(o);
                if (oc != null) {
                    switch (oc.objType) {
                        case "TABLE":
                            newFinalCreateObjects.add("DROP TABLE IF EXISTS " + oc.objName + ";");
                            break;
                        case "VIEW":
                            newFinalCreateObjects.add("DROP VIEW IF EXISTS " + oc.objName + ";");
                            break;
                    }
                }
            }
            newFinalCreateObjects.add("\n");
            for (String o : entry.getValue()) {
                oc = allObjectsMapList1.get(o);
                if (oc != null) {
                    newFinalCreateObjects.add(oc.objDdl + ";\n");
                }
            }
        }
        //System.out.println("newFinalCreateObjects:" + newFinalCreateObjects);
        SQLFileReader.AppendToFile("src/test/resources/outputFiles", newFinalCreateObjects);
    }

    public LinkedHashMap<String, LinkedList<String>> linageListToMap(LinkedList<String> linageList) {

        String[] dbName;
        LinkedList<String> newLinkedList = null;

        if (linageList.size() != 0) {
            for (String name : linageList) {
                dbName = name.split("\\.");

                if (dbName.length == 2) {
                    if (stringLinkedListLinkedHashMap.get(dbName[0]) == null) {
                        newLinkedList = new LinkedList<>();
                    } else {
                        newLinkedList = new LinkedList<>(stringLinkedListLinkedHashMap.get(dbName[0]));
                    }
                    newLinkedList.add(name);
                    stringLinkedListLinkedHashMap.put(dbName[0], newLinkedList);
                }
            }
        }
        return stringLinkedListLinkedHashMap;
    }

    @Override
    public void convertLane(TTable tt, String newSuffix) {

        final Matcher m = pattern.matcher(tt.toString());

        if (m.find()) {
            System.err.println("Already suffix <" + m.group(2) + m.group(3) + m.group(4) + "> found in tablename " + tt);
        } else {
            tt.getTableName().getObjectToken().astext = tt + "_" + newSuffix;
        }
        /*if (m.find()) {
                    System.err.println("Already suffix <" + m.group(2) + m.group(3) + m.group(4) + "> found in " +
                            "tablename " + t);
                } else {
                    t.getTableName().getObjectToken().astext = t + "_tl3";
                }*/
    }

    @Override
    public void extractTablenamesFromCreateView(String var) {

    }

    @Override
    public void extractTablenamesFromCreateTable(String var) {

    }

    @Override
    public void extractTablenamesFromSQL(String var) {

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