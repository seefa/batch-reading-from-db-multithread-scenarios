package ir.seefa.batch;

import ir.seefa.mapper.CustomerRowMapper;
import ir.seefa.model.Customer;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.database.PagingQueryProvider;
import org.springframework.batch.item.database.builder.JdbcPagingItemReaderBuilder;
import org.springframework.batch.item.database.support.SqlPagingQueryProviderFactoryBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import javax.sql.DataSource;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author Saman Delfani
 * @version 1.0
 * @since 2022-07-30 23:28:26
 */
@Configuration
public class ChuckBasedJob {

    private final JobBuilderFactory jobBuilderFactory;
    private final StepBuilderFactory stepBuilderFactory;
    private final AtomicInteger numberOfCustomers = new AtomicInteger();
    private final DataSource dataSource;

    public ChuckBasedJob(JobBuilderFactory jobBuilderFactory, StepBuilderFactory stepBuilderFactory, DataSource dataSource) {
        this.jobBuilderFactory = jobBuilderFactory;
        this.stepBuilderFactory = stepBuilderFactory;
        this.dataSource = dataSource;
    }


    @Bean
    public PagingQueryProvider queryProvider() throws Exception {
        SqlPagingQueryProviderFactoryBean factory = new SqlPagingQueryProviderFactoryBean();
        factory.setSelectClause("SELECT customerNumber, customerName, contactLastName, contactFirstName, phone, addressLine1, addressLine2, city, state, postalCode, country, salesRepEmployeeNumber, creditLimit");
        factory.setFromClause("FROM `spring-batch`.customers");
        factory.setSortKey("customerNumber");
        factory.setDataSource(dataSource);
        return factory.getObject();
    }

    @Bean
    public ItemReader<Customer> itemReader() throws Exception {
        return new JdbcPagingItemReaderBuilder<Customer>()
                .dataSource(dataSource)
                .name("jdbcPagingItemReader")
                .queryProvider(queryProvider())
                .pageSize(10)                           // MUST be equal to chunk size
                .rowMapper(new CustomerRowMapper())
                .build();
    }

    @Bean
    public Step chunkBasedReadingFlatFileStep() throws Exception {
        return this.stepBuilderFactory.get("chunkBasedReadingFromDBInMultiThreadScenarios")
                .<Customer, Customer>chunk(10)                  // Must be equal to queryProvider page size
                .reader(itemReader())
                .writer(items -> {
                    numberOfCustomers.getAndAdd(items.size());
                    items.forEach(System.out::println);
                    System.out.println("Number of Customers: " + numberOfCustomers.get());
                })
                .build();
    }

    @Bean
    public Job chuckOrientedJob() throws Exception {
        return this.jobBuilderFactory.get("chunkOrientedReadingDbInMultiThreadScenariosJob")
                .start(chunkBasedReadingFlatFileStep())
                .build();

    }
}
