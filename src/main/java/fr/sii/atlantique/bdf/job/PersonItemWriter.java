package fr.sii.atlantique.bdf.job;

import org.springframework.batch.item.database.JdbcBatchItemWriter;

import java.util.List;

public class PersonItemWriter extends JdbcBatchItemWriter<Person> {

    @Override
    public void write(List<? extends Person> items) throws Exception {
        Thread.sleep(40L);
        super.write(items);
    }
}
