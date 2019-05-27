import java.util.LinkedList;
import java.util.List;
import java.util.SortedSet;

public class DdlProperty {

    private String ObjectType, ObjectName;
    // Create the sorted set
    List<String> baseTableSet = new LinkedList<>();


    public List<String> getBaseTableSet() {
        return baseTableSet;
    }

    public void setBaseTableSet(List<String> baseTableSet) {
        this.baseTableSet = baseTableSet;
    }
}
