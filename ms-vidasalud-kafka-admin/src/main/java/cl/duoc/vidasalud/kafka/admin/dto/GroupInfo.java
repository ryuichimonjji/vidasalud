package cl.duoc.vidasalud.kafka.admin.dto;

import java.util.Map;

public class GroupInfo {

    private String groupId;
    private String state;
    private int members;
    private Map<String, Long> offsets;
    private Map<String, Long> endOffsets;
    private Map<String, Long> lag;
    private long totalLag;

    public GroupInfo() {}

    public GroupInfo(String groupId, String state, int members, Map<String, Long> offsets,
                     Map<String, Long> endOffsets, Map<String, Long> lag, long totalLag) {
        this.groupId = groupId;
        this.state = state;
        this.members = members;
        this.offsets = offsets;
        this.endOffsets = endOffsets;
        this.lag = lag;
        this.totalLag = totalLag;
    }

    public String getGroupId() { return groupId; }
    public String getState() { return state; }
    public int getMembers() { return members; }
    /** Ultimo offset commiteado por el group, por topico-particion. */
    public Map<String, Long> getOffsets() { return offsets; }
    /** Ultimo offset disponible en el broker, por topico-particion. */
    public Map<String, Long> getEndOffsets() { return endOffsets; }
    /** endOffset - offset commiteado: mensajes pendientes de consumir. */
    public Map<String, Long> getLag() { return lag; }
    public long getTotalLag() { return totalLag; }
}
