package do_.kafka;

import java.util.List;
import java.util.Objects;

/**
 * Metadata about a consumer group.
 */
public final class ConsumerGroupMetadata {
    private final String groupId;
    private final String state;
    private final List<String> members;
    private final int coordinator;

    /**
     * Creates consumer group metadata.
     */
    public ConsumerGroupMetadata(String groupId, String state, List<String> members, int coordinator) {
        this.groupId = groupId;
        this.state = state;
        this.members = members != null ? List.copyOf(members) : List.of();
        this.coordinator = coordinator;
    }

    /**
     * Returns the consumer group ID.
     */
    public String groupId() {
        return groupId;
    }

    /**
     * Returns the current state of the group.
     */
    public String state() {
        return state;
    }

    /**
     * Returns the list of member IDs.
     */
    public List<String> members() {
        return members;
    }

    /**
     * Returns the broker ID of the group coordinator.
     */
    public int coordinator() {
        return coordinator;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof ConsumerGroupMetadata that)) return false;
        return coordinator == that.coordinator &&
                Objects.equals(groupId, that.groupId) &&
                Objects.equals(state, that.state);
    }

    @Override
    public int hashCode() {
        return Objects.hash(groupId, state, coordinator);
    }

    @Override
    public String toString() {
        return "ConsumerGroupMetadata{" +
                "groupId='" + groupId + '\'' +
                ", state='" + state + '\'' +
                ", members=" + members +
                ", coordinator=" + coordinator +
                '}';
    }
}
