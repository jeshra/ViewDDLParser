import gudusoft.gsqlparser.nodes.TTable;

public interface HiveDdl {

    void convertLane(TTable tt, String newSuffix);

    void extractTablenamesFromCreateView(String var);

    void extractTablenamesFromCreateTable(String var);

    void extractTablenamesFromSQL(String var);


}
