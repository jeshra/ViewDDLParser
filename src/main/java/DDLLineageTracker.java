import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.operators.relational.Between;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.Statements;
import net.sf.jsqlparser.statement.create.view.CreateView;
import net.sf.jsqlparser.statement.select.SelectBody;
import net.sf.jsqlparser.statement.select.TableFunction;
import net.sf.jsqlparser.util.TablesNamesFinder;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public class DDLLineageTracker {
    static int i = 0;
    private static List<String> linkedNames = new ArrayList<>();
    private static Map<String, List<String>> stringListTreeMapTables = new TreeMap<>();
    private static List<String> LineageCreateTableList = new ArrayList<>();
    private static List<String> NonLineageCreateTableList = new ArrayList<>();
    //System.out.println("NonLineageCreateTableList->" + NonLineageCreateTableList.toString());
    private static StringBuilder stringBuilder = new StringBuilder();

    public static void main(String[] args) throws JSQLParserException {
        basicMain();

    }

    private static void basicMain() throws JSQLParserException {
        Statements statements;
        String sqlFile = "src/main/resources/SampleSQL.txt";
        String test;
        test = SQLFileReader.fileToString(sqlFile).toLowerCase();
        statements = CCJSqlParserUtil.parseStatements(test);
        int i = 0;
        for (Statement statement : statements.getStatements()) {
            findTableFromView(i, statement);
            i = i + 1;
        }

        System.out.println("stringListTreeMapTables.toString():" + stringListTreeMapTables.toString());
        NonLineageCreateTableList.addAll(stringListTreeMapTables.keySet());
        //processLineage(stringListTreeMapTables);
        for (String Table : NonLineageCreateTableList) {
            System.out.println("Processing Create view: " + Table);
            processLineage2(Table, stringListTreeMapTables);
        }
        System.out.println("LineageCreateTableList->" + LineageCreateTableList.toString());
        /**
         * Example Input
         * vemployee2=[vemployee5],
         * vemployee4=[vemployee3],
         * vemployee5=[vemployee4, vemployee3],
         * vemployee6=[vemployee4, vemployee3],
         * vemployee7=[vemployee8, vemployee5],
         * vemployee8=[vemployee9]
         *
         * Expected Output: LineageCreateTableList->[vemployee4, vemployee5, vemployee2, vemployee6, vemployee8, vemployee7]
         */

        linkedNames.toString();
    }

    private static void findTableFromView(int iteration, Statement statement) throws JSQLParserException {
        CreateView createView = (CreateView) statement;
        TablesNamesFinderExt tablesNamesFinder = new TablesNamesFinderExt();
        SelectBody select = createView.getSelectBody();//.getSelect();
        Statement s1 = CCJSqlParserUtil.parse(select.toString());
        stringListTreeMapTables.put(createView.getView().getName(), tablesNamesFinder.getTableList(s1));
    }

    private static void processLineage2(String Table, Map<String, List<String>> tableMap) {
        /*if (i == 0) {
            LineageCreateTableList.add(Table);
        }*/
        //System.out.println("NonLineageCreateTableList.toString():" + NonLineageCreateTableList.toString());


        if ((NonLineageCreateTableList.indexOf(Table)) != -1) {
            System.out.println(Table + ": Table is A create view");

            List<String> baseTableList = tableMap.get(Table);
            //System.out.println("List<String> baseTableList: " + baseTableList);


            for (String baseTable : baseTableList) {
                stringBuilder.append(baseTable).append("->");
                processLineage2(baseTable, tableMap);
            }
            if (LineageCreateTableList.indexOf(Table) == -1) LineageCreateTableList.add(Table);
            stringBuilder.append(Table).append("#");
            System.out.println(stringBuilder.toString());
        } else System.out.println(Table + ": Table is NOT A create view");

        /*if ((NonLineageCreateTableList.indexOf(Table)) != -1) {

        }

        for (Map.Entry<String, List<String>> ee : tableMap.entrySet()) {
            String key = ee.getKey();
            List<String> values = ee.getValue();
            for (String baseTable : values) {
                processLineage2(key, baseTable);


            }
        }*/
    }

    static class TablesNamesFinderExt extends TablesNamesFinder {
        //            @Override
//            public void visit(Column tableColumn) {
//                System.out.println("column = " + tableColumn);
//            }

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
