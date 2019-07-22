import gudusoft.gsqlparser.nodes.TTable;

public interface HiveDdl {

    /**
     * What are the action this framework will do?
     * 1. Lane checker & Lister. Should find out any of lanes in file that are going to Prod, uat, sit, dev
     * 2. *Lane converter(Also report lanelist), overwrites regardless of any different exiting lanes to new given lane
     * 3. Extract Tablename for/from Create view(s)
     * 4. Extract Tablename for/from Create Table
     * 5. Extract Tablename for/from Select SQL
     * 6. List all type of statements with name (drop tbl, create tbl, ....) Report
     * 7. *Lineage order DDL sorter
     * 8. Format DDLs
     */

    void convertLane(TTable tt, String newSuffix);

    void extractTablenamesFromCreateView(String var);

    void extractTablenamesFromCreateTable(String var);

    void extractTablenamesFromSQL(String var);


}
