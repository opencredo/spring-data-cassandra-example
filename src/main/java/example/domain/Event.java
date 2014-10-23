package example.domain;

import org.springframework.cassandra.core.Ordering;
import org.springframework.cassandra.core.PrimaryKeyType;
import org.springframework.data.cassandra.mapping.Column;
import org.springframework.data.cassandra.mapping.PrimaryKeyColumn;
import org.springframework.data.cassandra.mapping.Table;

import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

@Table
public class Event {

    @PrimaryKeyColumn(name = "id", ordinal = 2, type = PrimaryKeyType.CLUSTERED, ordering = Ordering.DESCENDING)
    private final UUID id;

    @PrimaryKeyColumn(name = "type", ordinal = 0, type = PrimaryKeyType.PARTITIONED)
    private final String type;

    @PrimaryKeyColumn(name = "bucket", ordinal = 1, type = PrimaryKeyType.PARTITIONED)
    private final String bucket;

    @Column
    private final Set<String> tags = new HashSet<>();

    public Event(UUID id, String type, String bucket, Set<String> tags) {
        this.id = id;
        this.type = type;
        this.bucket = bucket;
        this.tags.addAll(tags);
    }

    public UUID getId() {
        return id;
    }

    public String getType() {
        return type;
    }

    public String getBucket() {
        return bucket;
    }

    public Set<String> getTags() {
        return tags;
    }

    @Override
    public String toString() {
        return "Event{" + "id=" + id + ", type=" + type + ", bucket=" + bucket + ", tags=" + tags + '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Event event = (Event) o;

        if (!bucket.equals(event.bucket)) return false;
        if (!id.equals(event.id)) return false;
        if (!tags.equals(event.tags)) return false;
        if (!type.equals(event.type)) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = id.hashCode();
        result = 31 * result + type.hashCode();
        result = 31 * result + bucket.hashCode();
        result = 31 * result + tags.hashCode();
        return result;
    }
}
