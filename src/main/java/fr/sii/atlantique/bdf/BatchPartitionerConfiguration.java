package fr.sii.atlantique.bdf;

import fr.sii.atlantique.bdf.job.JobCompletionNotificationListener;
import fr.sii.atlantique.bdf.job.Person;
import fr.sii.atlantique.bdf.job.PersonItemProcessor;
import fr.sii.atlantique.bdf.job.PersonItemWriter;
import fr.sii.atlantique.bdf.partitioner.CustomPartitioner;
import fr.sii.atlantique.bdf.split.LineCountTasklet;
import fr.sii.atlantique.bdf.split.SplitItemWriter;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.StepExecutionListener;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.listener.ExecutionContextPromotionListener;
import org.springframework.batch.core.partition.support.Partitioner;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.database.BeanPropertyItemSqlParameterSourceProvider;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.mapping.PassThroughLineMapper;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.core.task.TaskExecutor;

import javax.sql.DataSource;

//@Configuration
public class BatchPartitionerConfiguration {

    private final JobBuilderFactory jobBuilderFactory;

    private final StepBuilderFactory stepBuilderFactory;

    private final DataSource dataSource;

    public BatchPartitionerConfiguration(JobBuilderFactory jobBuilderFactory, StepBuilderFactory stepBuilderFactory, DataSource dataSource) {
        this.jobBuilderFactory = jobBuilderFactory;
        this.stepBuilderFactory = stepBuilderFactory;
        this.dataSource = dataSource;
    }

    @Bean
    public Step lineCountStep(Tasklet lineCountTasklet, StepExecutionListener promotionListener) {
        return stepBuilderFactory.get("lineCountStep")
                .tasklet(lineCountTasklet)
                .listener(promotionListener)
                .build();
    }

    @Bean
    public Tasklet lineCountTasklet() {
        LineCountTasklet tasklet = new LineCountTasklet();
        tasklet.setResource(new ClassPathResource("sample-data.csv"));
        return tasklet;
    }

    @Bean
    public StepExecutionListener promotionListener() {
        ExecutionContextPromotionListener promotionListener = new ExecutionContextPromotionListener();
        promotionListener.setKeys(new String[]{"line.count"});
        return promotionListener;
    }

    @Bean
    public ItemReader<String> splitReader() {
        FlatFileItemReader<String> reader = new FlatFileItemReader<>();
        reader.setResource(new ClassPathResource("sample-data.csv"));
        reader.setLineMapper(new PassThroughLineMapper());
        return reader;
    }

    @Bean
    @StepScope
    public SplitItemWriter splitWriter(@Value("#{jobExecutionContext['line.count']}") int lineCount) {
        SplitItemWriter splitItemWriter = new SplitItemWriter();
        splitItemWriter.setInputLineCount(lineCount);
        splitItemWriter.setNbFiles(10);
        return splitItemWriter;
    }

    @Bean
    public CustomPartitioner partitioner() {
        return new CustomPartitioner();
    }

    @Bean
    @StepScope
    public FlatFileItemReader<Person> reader(@Value("#{stepExecutionContext[file]}") String filename) {
        FlatFileItemReader<Person> reader = new FlatFileItemReader<>();
        reader.setResource(new FileSystemResource("src/main/resources/input/partitioner/" + filename));
        reader.setLineMapper(new DefaultLineMapper<Person>() {{
            setLineTokenizer(new DelimitedLineTokenizer() {{
                setNames(new String[] { "firstName", "lastName" });
            }});
            setFieldSetMapper(new BeanWrapperFieldSetMapper<Person>() {{
                setTargetType(Person.class);
            }});
        }});
        return reader;
    }

    @Bean
    public PersonItemProcessor processor() {
        return new PersonItemProcessor();
    }

    @Bean
    public JdbcBatchItemWriter<Person> writer() {
        PersonItemWriter writer = new PersonItemWriter();
        writer.setItemSqlParameterSourceProvider(new BeanPropertyItemSqlParameterSourceProvider<>());
        writer.setSql("INSERT INTO people (first_name, last_name) VALUES (:firstName, :lastName)");
        writer.setDataSource(dataSource);
        return writer;
    }


    @Bean
    public Job partitionerJob(Step lineCountStep, Step splitStep, Step partitionStep, JobCompletionNotificationListener listener) {
        return jobBuilderFactory.get("partitioningJob")
                .start(lineCountStep)
                .next(splitStep)
                .next(partitionStep)
                .listener(listener)
                .build();
    }

    @Bean
    public Step splitStep(ItemReader<String> splitReader, ItemWriter<String> splitWriter) {
        return stepBuilderFactory.get("splitStep")
                .<String, String> chunk(10)
                .reader(splitReader)
                .writer(splitWriter)
                .build();
    }

    @Bean
    public Step partitionStep(Step step, Partitioner partitioner) {
        return stepBuilderFactory.get("partitionStep")
                .partitioner("slaveStep", partitioner)
                .step(step)
                .taskExecutor(taskExecutor())
                .build();
    }

    @Bean
    public Step step(ItemReader<Person> reader) {
        return stepBuilderFactory.get("step")
                .<Person, Person> chunk(1)
                .reader(reader)
                .processor(processor())
                .writer(writer())
                .build();
    }

    @Bean
    public TaskExecutor taskExecutor() {
        SimpleAsyncTaskExecutor taskExecutor = new SimpleAsyncTaskExecutor();
        taskExecutor.setConcurrencyLimit(10);
        return taskExecutor;
    }
}
