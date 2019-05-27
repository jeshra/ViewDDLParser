import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.util.TablesNamesFinder;

public class TableFinderTrail {


    public static void main(String[] args) {
        findTable("SELECT 'name:'||e.name,\n" +
                "       'id:'||e.department_id,\n" +
                "       'name:'||d.name,\n" +
                "       'loc:'||loc\n" +
                "FROM   wds.employees e\n" +
                "       LEFT OUTER JOIN wds.department d\n" +
                "         ON ( e.department_id = d.department_id );");
    }


    public static void findTable(String sql) {
        Statement statement = null;
        Table t = null;
        try {
            statement = CCJSqlParserUtil.parse(sql);
            t = (Table) CCJSqlParserUtil.parse(sql);
        } catch (JSQLParserException e) {
            e.printStackTrace();
        }
        Select selectStatement = (Select) statement;
        TablesNamesFinder tablesNamesFinder = new TablesNamesFinder() {
            /*@Override
            public void visit(Column tableColumn) {
                System.out.println(tableColumn);
            }*/
        };
        System.out.println("Tables=" + tablesNamesFinder.getTableList(selectStatement));

        System.out.println(t.getSchemaName());

    }
}
