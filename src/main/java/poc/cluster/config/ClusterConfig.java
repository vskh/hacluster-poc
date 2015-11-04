package poc.cluster.config;

import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.ImportResource;

/**
 * Cluster Spring configuration
 *
 * @author vadya
 */
@Configuration
@ComponentScan(basePackages = { "poc.cluster.ha" })
@ImportResource("classpath:cluster-backend-config.xml")
public class ClusterConfig {

}
