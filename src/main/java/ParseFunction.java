import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.operators.relational.Between;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.Statements;
import net.sf.jsqlparser.statement.create.view.CreateView;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.TableFunction;
import net.sf.jsqlparser.util.TablesNamesFinder;

import java.util.*;

public class ParseFunction {
    private static List<String> tableNames = new ArrayList<>();//LinkedList<>();
    private static List<String> linkedNames = new ArrayList<>();
    private static Map<String, List<String>> m = new HashMap<>();
    private static Map<String, List<String>> stringListTreeMapTables = new TreeMap<>();
    private static List<String> lineageTables = new ArrayList<>();

    public static void main(String[] args) throws JSQLParserException {
        basicMain();

    }

    public static String basicMain() throws JSQLParserException {
        Statements statements;
        String sqlFile = "src/main/resources/SampleSQL.txt";

        SQLFileReader sqlFileReader = SQLFileReader.getInstance();
        String test;
        test = SQLFileReader.fileToString(sqlFile).toLowerCase();

        statements = CCJSqlParserUtil.parseStatements(test);
        //DdlProperty ddlProperty = new DdlProperty();
        int i = 0;
        for (Statement statement : statements.getStatements()) {
            TablesNamesFinderExt tablesNamesFinder = new TablesNamesFinderExt();

            findTableFromView(i, statement);
            //System.out.println("all extracted tables=" + tablesNamesFinder.getTableList(statement));
            //System.out.println("\n\n");
            i = i + 1;
        }

        //Collections.swap(tableNames, 2, 1);
        //System.out.println(tableNames.toString());
        //System.out.println(linkedNames.toString());
        System.out.println(stringListTreeMapTables.toString());
        processLineage(stringListTreeMapTables);
        System.out.println("lineageTables->" + lineageTables.toString());
        return linkedNames.toString();
    }

    private static void findTableFromView(int iteration, Statement statement) {
        CreateView createView1 = (CreateView) statement;
        TablesNamesFinderExt tablesNamesFinder = new TablesNamesFinderExt();
        Select select = createView1.getSelect();

        stringListTreeMapTables.put(createView1.getView().getName(), tablesNamesFinder.getTableList(select));

    }

    static public void processLineage(Map<String, List<String>> tableMap) {
        List<String> orderFixedTableList = new ArrayList<>();
        List<String> createTables = new ArrayList<>(tableMap.keySet());

        System.out.println("createTables->" + createTables.toString());
        int i = 0;
/*        for (Entry<String, List<Integer>> ee : map.entrySet()) {
            String key = ee.getKey();
            List<Integer> values = ee.getValue();

        }*/
        String createViewTableName, createViewBaseTableName;
        for (String f_viewName : tableMap.keySet()) {
            createViewTableName = f_viewName;
            if (i == 0) {
                lineageTables.add(f_viewName);
            } else if (lineageTables.indexOf(createViewTableName) == -1) lineageTables.add(createViewTableName);
            System.out.println(">>>lineageTables.toString()->" + lineageTables.toString());


            List<String> baseTableList = tableMap.get(f_viewName);
            System.out.println("tableMap.get(" + f_viewName + ")->" + baseTableList.toString());
            int ind;
            for (String baseTable : baseTableList) {
                System.out.println("forLoop of baseTable: " + baseTable);
                if ((ind = createTables.indexOf(baseTable)) != -1) {
                    int createViewIndex = -1;
                    System.out.println("BBEFORE lineageTables add:" + lineageTables.toString());
                    if (lineageTables.indexOf(baseTable) == -1)
                        lineageTables.add(baseTable);


                    createViewIndex = lineageTables.indexOf(f_viewName);
                    System.out.println("AAFTER lineageTables add:" + lineageTables.toString());
                    //TODO: recursive function call as like in Scala
                    int baseTableIndex = lineageTables.indexOf(baseTable);
                    System.out.println("ind->" + ind + ", createViewIndex->" + createViewIndex + ", baseTableIndex" +
                            "->" + baseTableIndex);
                    System.out.println("BEFORE swap of linkedNames (" + i + ")->" + lineageTables.toString());
                    // if first time, no need to check in orderFixedTableList
                    if (i == 0) {
                        Collections.swap(lineageTables, createViewIndex, baseTableIndex);
                        //orderFixedTableList.addAll(lineageTables);
                    } else {
                        // if not first time, check orderFixedTableList, already in sorted/swaped or not.

                        if (lineageTables.indexOf(f_viewName) < lineageTables.indexOf(baseTable)) {
                            Collections.swap(lineageTables, createViewIndex, baseTableIndex);
                        }
                    }

                    System.out.println("AFTER swap of linkedNames (" + i + ")->" + lineageTables.toString());
                }
            }
            //}
            System.out.println("\n");
            i = i + 1;
        }
    }

    public static void findTableFromView1(int iteration, Statement statement) {
        CreateView createView1 = (CreateView) statement;
        System.out.println("VIEW:\t" + createView1.isOrReplace());
        Select select = createView1.getSelect();
        TablesNamesFinderExt tablesNamesFinder = new TablesNamesFinderExt();
        System.out.println("ViewName:\t" + createView1.getView().getName());

        tableNames.add(createView1.getView().getName());


        //System.out.println("createView:" + createView1.getSelect());
        System.out.println("AllTables:\t" + tablesNamesFinder.getTableList(select) + "\n");
        System.out.println("iteration->" + iteration);
        if (iteration == 0) {
            linkedNames.addAll(tablesNamesFinder.getTableList(select));
            linkedNames.add(createView1.getView().getName());
        } else {
            List<String> tempList = new ArrayList<>(tablesNamesFinder.getTableList(select));
            tempList.add(createView1.getView().getName());
            int[] linkedNamesIndex = new int[tempList.size()];

            System.out.println("tempList.size()->" + tempList.size() + " :: linkedNamesIndex.length->" + linkedNamesIndex.length + " :: linkedNames.size()->" + linkedNames.size());
            int numberofDependencyFound = -1;
            for (int k = 0; k < tempList.size(); k++) {
                linkedNamesIndex[k] = linkedNames.indexOf(tempList.get(k));
                System.out.println("i[" + k + "]->" + linkedNamesIndex[k]);

                if (linkedNamesIndex[k] != -1) numberofDependencyFound = numberofDependencyFound + 1;

            }


            for (int k = 0; k <= linkedNamesIndex.length; k++) {
                System.out.println("Something i[k+1]->" + linkedNamesIndex[k + 1]);
                System.out.println("Last Loop i[" + k + "]->" + linkedNamesIndex[k]);
                if (numberofDependencyFound > 0 & (linkedNamesIndex[k] > linkedNamesIndex[k + 1])) {
                    System.out.println("before linkedNames->" + linkedNames.toString());
                    Collections.swap(linkedNames, linkedNamesIndex[k], linkedNamesIndex[k + 1]);
                    System.out.println("after linkedNames->" + linkedNames.toString());
                } else {
                    //linkedNames.addAll(tempList);
                    for (String s : tempList) {
                        if (linkedNames.indexOf(s) == -1)
                            linkedNames.add(s);
                    }
                }
            }
        }
    }

    static class TablesNamesFinderExt extends TablesNamesFinder {
        List<String> mySelectTableList = new ArrayList<>();
        boolean inSelect = true;
        //            @Override
//            public void visit(Column tableColumn) {
//                System.out.println("column = " + tableColumn);
//            }

        /**
         * Helps to find user or system functions from statement
         *
         * @param function
         */
        @Override
        public void visit(Function function) {
            ExpressionList exprList = function.getParameters();
            if (exprList != null) {
                visit(exprList);
            }
            //System.out.println("function = " + function.getName());
            super.visit(function);
        }


        @Override
        public void visit(Table tableName) {
            //System.out.println("BaseTable:\t" + tableName.getFullyQualifiedName());
            //System.out.println("table getDatabase = " + tableName.getSchemaName());
            //System.out.println(statement.);
            super.visit(tableName);
        }

        @Override
        public void visit(TableFunction valuesList) {
            System.out.println("table function = " + valuesList.getFunction().getName());
            super.visit(valuesList);
        }


        @Override
        public void visit(Between between) {
            System.out.println("Boolean start:" + between.getBetweenExpressionStart());

            System.out.println("Boolean end:" + between.getBetweenExpressionEnd());
            System.out.println("Boolean getLeftExpression:" + between.getLeftExpression());
            super.visit(between);
        }
    }

}
