package example;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;
import com.datastax.driver.core.utils.UUIDs;
import com.google.common.collect.ImmutableSet;
import example.domain.Event;
import example.domain.EventRepository;
import org.springframework.cassandra.core.CqlOperations;
import org.springframework.cassandra.core.cql.CqlIdentifier;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.data.cassandra.core.CassandraAdminOperations;
import org.springframework.data.cassandra.core.CassandraOperations;

import java.util.HashMap;
import java.util.List;

public class SpringDataCassandraExample {

    public static final String TIME_BUCKET = "2014-01-01";

    public static void main(String[] args) {
        ApplicationContext context = new AnnotationConfigApplicationContext(CassandraConfiguration.class);
        CassandraAdminOperations template = context.getBean(CassandraAdminOperations.class);

        resetKeySpace(template);

        Select select = useCqlTemplate((CqlOperations) template);
        useCassandraTemplate((CassandraOperations) template, select);
        useCassandraRepository(context);
    }

    private static Select useCqlTemplate(CqlOperations cqlTemplate) {
        cqlTemplate.execute("insert into event (id, type, bucket, tags) values (" + UUIDs.timeBased() + ", 'type1', '" + TIME_BUCKET + "', {'tag2', 'tag3'})");

        Insert insert1 = QueryBuilder.insertInto("event").value("id", UUIDs.timeBased()).value("type", "type2")
                .value("bucket", TIME_BUCKET).value("tags", ImmutableSet.of("tag1"));
        cqlTemplate.execute(insert1);

        Statement insert2 = cqlTemplate.getSession().prepare("insert into event (id, type, bucket, tags) values (?, ?, ?, ?)")
                .bind(UUIDs.timeBased(), "type2", TIME_BUCKET, ImmutableSet.of("tag1", "tag2"));
        cqlTemplate.execute(insert2);

        ResultSet rs;
        rs = cqlTemplate.query("select * from event where type='type2' and bucket='" + TIME_BUCKET + "'");
        for (Row row : rs.all()) {
            System.out.println(row);
        }

        Select select = QueryBuilder.select().from("event").where(QueryBuilder.eq("type", "type1")).and(QueryBuilder.eq("bucket", TIME_BUCKET)).limit(10);
        rs = cqlTemplate.query(select);
        for (Row row : rs.all()) {
            System.out.println(row);
        }
        return select;
    }

    private static void useCassandraTemplate(CassandraOperations cassandraTemplate, Select select) {
        cassandraTemplate.insert(new Event(UUIDs.timeBased(), "type3", TIME_BUCKET, ImmutableSet.of("tag1", "tag3")));

        Event oneEvent = cassandraTemplate.selectOne(select, Event.class);
        System.out.println(oneEvent);

        List<Event> someEvents = cassandraTemplate.select(select, Event.class);
        System.out.println(someEvents);
    }

    private static void useCassandraRepository(ApplicationContext context) {
        EventRepository repository = context.getBean(EventRepository.class);
        repository.save(new Event(UUIDs.timeBased(), "type1", TIME_BUCKET, ImmutableSet.of("tag1", "tag2")));

        Iterable<Event> moreEvents = repository.findByTypeAndBucket("type1", TIME_BUCKET);
        System.out.println(moreEvents);

        repository.delete(moreEvents.iterator().next());
    }

    private static void resetKeySpace(CassandraAdminOperations template) {
        template.dropTable(CqlIdentifier.cqlId("event"));
        template.createTable(true, CqlIdentifier.cqlId("event"), Event.class, new HashMap<String, Object>());
    }
}
