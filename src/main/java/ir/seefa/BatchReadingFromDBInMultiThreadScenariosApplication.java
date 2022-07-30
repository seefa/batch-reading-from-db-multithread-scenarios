package ir.seefa;

import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * @author Saman Delfani
 * @version 1.0
 * @since 2022-07-30 23:28:02
 */
@SpringBootApplication
@EnableBatchProcessing
public class BatchReadingFromDBInMultiThreadScenariosApplication {
    public static void main(String[] args) {
        SpringApplication.run(BatchReadingFromDBInMultiThreadScenariosApplication.class, args);
    }
}
