package com.neo4j.examples.sdn6_reactive_multidatabase;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import org.neo4j.driver.Driver;
import org.neo4j.driver.SessionConfig;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.data.neo4j.Neo4jDataProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.domain.Sort;
import org.springframework.data.neo4j.core.DatabaseSelection;
import org.springframework.data.neo4j.core.ReactiveDatabaseSelectionProvider;
import org.springframework.data.neo4j.core.ReactiveNeo4jClient;
import org.springframework.data.neo4j.core.schema.GeneratedValue;
import org.springframework.data.neo4j.core.schema.Id;
import org.springframework.data.neo4j.core.schema.IdGenerator;
import org.springframework.data.neo4j.core.schema.Node;
import org.springframework.data.neo4j.core.schema.Property;
import org.springframework.data.neo4j.core.schema.Relationship;
import org.springframework.data.neo4j.core.transaction.ReactiveNeo4jTransactionManager;
import org.springframework.data.neo4j.repository.ReactiveNeo4jRepository;
import org.springframework.data.neo4j.repository.config.EnableReactiveNeo4jRepositories;
import org.springframework.data.neo4j.repository.query.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Service;
import org.springframework.transaction.ReactiveTransactionManager;
import org.springframework.transaction.annotation.Transactional;

/**
 * This application uses the dataset and the setup explained here
 * <a href="https://neo4j.com/developer/multi-tenancy-worked-example/">Multi Tenancy in Neo4j: A Worked Example</a>
 *
 * <p>Disregarding best practices in how to structure an application for the benefit to have everything in place, we
 * do configure everything here in this class and also define our SDN entities here, so that you can read this example from
 * top to bottom.
 *
 * <p>In <code>application.properties</code> configure a database user that has access rights to
 *
 * <ul>
 *     <li>mall1</li>
 *     <li>mall2</li>
 *     <li>fabric</li>
 * </ul>
 *
 * <p>We can make the fabric database the default database. If we do so, write queries that miss the context, are
 * guaranteed to fail, which is probably what we want.
 *
 * <p>Make the properties file look like this:
 *
 * <pre>
 * 	spring.neo4j.authentication.username=neo4j
 * 	spring.neo4j.authentication.password=secret
 * 	spring.neo4j.uri=bolt://localhost:7687
 * 	spring.data.neo4j.database=fabric
 * </pre>
 */
@SpringBootApplication(proxyBeanMethods = false)
// Only needed due to the fact that we have our repositories in this class.
@EnableReactiveNeo4jRepositories(considerNestedRepositories = true)
public class Application implements CommandLineRunner {

	/**
	 * A constant that is used in various Reactor contexts to specify the selected database name.
	 */
	public static final String KEY_DATABASE_NAME = "database";

	public static void main(String[] args) {
		SpringApplication.run(Application.class, args);
	}

	// Some configuration
	@Configuration(proxyBeanMethods = false)
	static class Neo4jConfig {
		/**
		 * We want to save some clients to see that write operations are going to the correct database.
		 * Here is one way to have some kinda of sequence generator.
		 *
		 * @param driver
		 * @return
		 */
		@Bean
		public SequenceGenerator clientSequenceGenerator(Driver driver, Neo4jDataProperties neo4jDataProperties) {

			try (var session = driver.session(SessionConfig.forDatabase(neo4jDataProperties.getDatabase()))) {
				var maxIdOverAllMalls = session.readTransaction(tx ->
					tx.run("CALL {\n"
						   + "  USE mall1\n"
						   + "  MATCH (p:Client)\n"
						   + "  RETURN max(p.id) AS maxIdInMall\n"
						   + "  UNION \n"
						   + "  USE mall2\n"
						   + "  MATCH (p:Client)\n"
						   + "  RETURN max(p.id) AS maxIdInMall\n"
						   + "} \n"
						   + "return max(maxIdInMall);").single().get(0).asLong()
				);
				return new SequenceGenerator(maxIdOverAllMalls);
			}
		}

		/**
		 * This is the key point of providing a dynamic database selection based on the reactive application context.
		 * We check reactive context if it contains a key with the selected database. If it is not there, we make
		 * use of the fact that we can have the original Spring configuration injected here in this configuration bean
		 * and use the default database name as configured in `application.properties``
		 *
		 * @param neo4jDataProperties The original Spring Data Neo4j properties as provided by Spring Boot
		 * @return
		 */
		@Bean
		public ReactiveDatabaseSelectionProvider reactiveDatabaseSelectionProvider(
			Neo4jDataProperties neo4jDataProperties) {

			return () -> Mono.deferContextual(ctx ->
				Mono.justOrEmpty(ctx.<String>getOrEmpty(KEY_DATABASE_NAME))
					.map(DatabaseSelection::byName)
					.switchIfEmpty(Mono.just(DatabaseSelection.byName(neo4jDataProperties.getDatabase())))
			);
		}

		/**
		 * The reactive transaction manager is not automatically configured by Spring Boot due to some limitations of the
		 * framework. If you want to have {@link Transactional declarative and reactive @Transactional} methods, you need
		 * to provide it yourself.
		 *
		 * @return
		 */
		@Bean
		public ReactiveTransactionManager reactiveTransactionManager(Driver driver,
			ReactiveDatabaseSelectionProvider databaseSelectionProvider) {
			return new ReactiveNeo4jTransactionManager(driver, databaseSelectionProvider);
		}
	}

	/**
	 * The repositories don't know nothing about the database inside the reactive context.
	 * We can work with operators as {@link #inDatabase(String, Flux)} shown below or wrap in
	 * in a service as {@link ClientService} shows.
	 */
	interface ClientRepository extends ReactiveNeo4jRepository<Client, Long> {

		/**
		 * Example query from the blog post.
		 *
		 * @return
		 */
		@Query("MATCH (client:Client)-[:PURCHASED]->(ticket)-[:HAS_TICKETITEM]->(item:TicketItem)\n"
			   + "WHERE client.id <> \"Unknown\"\n"
			   + "WITH client, count(DISTINCT ticket) AS tickets,\n"
			   + "     apoc.math.round(sum(item.netAmount), 2) AS totalSpend\n"
			   + "RETURN client, totalSpend, tickets,\n"
			   + "       apoc.math.round(totalSpend / tickets, 2) AS spendPerTicket\n"
			   + "ORDER BY totalSpend DESC\n"
			   + "LIMIT 10")
		Flux<ClientStatistics> getTop10BiggestSpendingClients();
	}

	interface ProductRepository extends ReactiveNeo4jRepository<Product, Long> {

		/**
		 * Another example query from the guide.
		 *
		 * @param id
		 * @return
		 */
		@Query("MATCH path = (:Client {id: $id})-[:PURCHASED]->()-->(:TicketItem)-->(:Product) RETURN path")
		Flux<Product> findAllByClientId(@Param("id") Long id);

		/**
		 * This custom query uses the fabric database, which is the configured default database.
		 * Make sure you _don't_ use a Reactor context with the database name it, otherwise the query would fail.
		 *
		 * @return
		 */
		@Query("UNWIND fabric.graphIds() AS graphId\n"
			   + "CALL {\n"
			   + "  USE fabric.graph(graphId)\n"
			   + "  MATCH (item:TicketItem)-->(product:Product)\n"
			   + "  WITH product, apoc.math.round(sum(item.netAmount), 2) AS totalSpend,\n"
			   + "       count(*) AS purchases\n"
			   + "  RETURN product, totalSpend, purchases\n"
			   + "  ORDER BY totalSpend DESC\n"
			   + "}\n"
			   // The map here is needed to correctly aggregate products from different database
			   // and make SDN map it back to a product.
			   + "RETURN product{.description}, sum(totalSpend) AS totalSpend, sum(purchases) AS purchases\n"
			   + "ORDER BY totalSpend DESC")
		Flux<ProductStatistics> findTopSellingProductsAcrossMalls();
	}

	// The following two operators allow us to decorate the reactor context.

	static <T> Mono<T> inDatabase(String database, Mono<T> original) {
		return original.contextWrite(context -> context.put(KEY_DATABASE_NAME, database));
	}

	static <T> Flux<T> inDatabase(String database, Flux<T> original) {
		return original.contextWrite(context -> context.put(KEY_DATABASE_NAME, database));
	}

	// Infrastructure and entity beans follow at the bottom, here is now the sample application

	private final ClientRepository clientRepository;

	private final ProductRepository productRepository;

	private final ReactiveNeo4jClient neo4jClient;

	private final Neo4jDataProperties neo4jDataProperties;

	public Application(ClientRepository clientRepository, ProductRepository productRepository, ReactiveNeo4jClient neo4jClient, Neo4jDataProperties neo4jDataProperties) {
		this.clientRepository = clientRepository;
		this.productRepository = productRepository;
		this.neo4jClient = neo4jClient;
		this.neo4jDataProperties = neo4jDataProperties;
	}

	@Override
	public void run(String... args) throws Exception {

		// First try to select a client in the default database
		// Using the repository on purpose here. We didn't write anything to the context
		// about the database info, so it always goes to fabric

		// We use reactors test utilities to show what's going on.
		// These belong to the test scope usually.

		// Make sure we find nothing in fabric itself.
		clientRepository.findAll()
			.as(StepVerifier::create)
			.verifyComplete();

		// Let's see if we can save to fabric.
		clientRepository.save(new Client(List.of()))
			.as(StepVerifier::create)
			.expectErrorMatches(e -> e.getMessage().matches("Creating new node label is not allowed for user '.+'.*"))
			.verify();

		// Good, let's check if our orchestrated services goes into the right database
		var recorder = new ArrayList<Long>();
		inDatabase("mall1", clientRepository.save(new Client(List.of())))
			.map(Client::getId)
			.as(StepVerifier::create)
			.recordWith(() -> recorder)
			.expectNextCount(1)
			.verifyComplete();

		// Retrieve it back in the same database
		inDatabase("mall1", clientRepository.findById(recorder.get(0)))
			.map(Client::getId)
			.as(StepVerifier::create)
			.expectNextMatches(recorder::contains)
			.verifyComplete();

		// Make sure it isn't in the other one
		inDatabase("mall2", clientRepository.findById(recorder.get(0)))
			.as(StepVerifier::create)
			.verifyComplete();

		// Let's have a look at a whole graph and pick some random client
		inDatabase("mall2", clientRepository.findAll(Sort.unsorted()))
			.take(1) // One should be enough ;)
			.as(StepVerifier::create)
			.consumeNextWith(System.out::println)
			.verifyComplete();

		// Print out biggest spenders in mall1
		System.out.println("Biggest spenders in mall1");
		inDatabase("mall1", clientRepository.getTop10BiggestSpendingClients())
			.doOnNext(System.out::println)
			.as(StepVerifier::create)
			.expectNextCount(10)
			.verifyComplete();

		// Print out biggest spenders in mall2
		System.out.println("Biggest spenders in mall2");
		inDatabase("mall2", clientRepository.getTop10BiggestSpendingClients())
			.doOnNext(System.out::println)
			.as(StepVerifier::create)
			.expectNextCount(10)
			.verifyComplete();

		// Get the biggest spender from mall2 and fetch the product's they bought
		System.out.println("Biggest spender bought this:");
		inDatabase("mall2",
			clientRepository
				.getTop10BiggestSpendingClients()
				.take(1)
				.flatMap(c -> productRepository.findAllByClientId(c.id))
				.take(3)
		)
			.doOnNext(System.out::println)
			.as(StepVerifier::create)
			.expectNextCount(3)
			.verifyComplete();

		// last but not least we do run a query inside fabric to get the spendings across all malls.
		// this could also be added to a repository, but as those repositories are to be understood
		// as "a collection of things" as in the DDD book and that arbitrary value is not an entity on
		// it's own, we shouldn't treat it as one.
		// We do use the Neo4j client here as this the template would work on the domain model again.
		// We don't use the plain driver as that one wouldn't participate in Spring transactions
		System.out.println("Spendings per mall");
		this.neo4jClient.query(
			"WITH [\"Mall 1\", \"Mall 2\"] AS malls\n"
			+ "UNWIND fabric.graphIds() AS graphId\n"
			+ "CALL {\n"
			+ "  USE fabric.graph(graphId)\n"
			+ "  MATCH (item:TicketItem)-->(product:Product)\n"
			+ "  RETURN apoc.math.round(sum(item.netAmount), 2) AS totalSpend,\n"
			+ "         count(*) AS purchases\n"
			+ "}\n"
			+ "RETURN malls[graphId] AS mall, totalSpend, purchases\n"
			+ "ORDER BY totalSpend DESC"
		)
			.in(neo4jDataProperties
				.getDatabase()) // This is required! The client itself has no idea of the database provider
			// Remember, we configured fabric inside those properties.
			.fetch()
			.all()
			.doOnNext(System.out::println)
			.as(StepVerifier::create)
			.expectNextCount(2)
			.verifyComplete();

		// Run a fabric query via a repository.
		System.out.println("Topn 3 products accross all malls");
		this.productRepository.findTopSellingProductsAcrossMalls()
			.doOnNext(System.out::println)
			.take(3)
			.as(StepVerifier::create)
			.expectNextCount(3)
			.verifyComplete();

		// Let's see what happens when we do actually add a context
		// with a non-fabric database to the flow
		inDatabase("mall1", this.productRepository.findTopSellingProductsAcrossMalls())
			.as(StepVerifier::create)
			.expectErrorMatches(e -> e.getMessage().matches(
				"(?s).*Dynamic graph lookup not allowed here. This feature is only available in a Fabric database.*"))
			.verify();

		// Give the last transactions a bit to clean up
		Thread.sleep(500L);
		System.exit(0);
	}

	static class SequenceGenerator implements IdGenerator<Long> {

		private final AtomicLong sequence;

		SequenceGenerator(Long startValue) {
			this.sequence = new AtomicLong(startValue);
		}

		@Override
		public Long generateId(String primaryLabel, Object entity) {
			return this.sequence.incrementAndGet();
		}
	}

	@Node
	static class Client {

		/**
		 * Assigned id (an actual property and not the identy function).
		 * Assigned means that is assigned from the client side and in this
		 * case, assigned with a generated value, orchestrated by SDN 6.
		 */
		@Id
		@GeneratedValue(generatorRef = "clientSequenceGenerator")
		private Long id;

		@Relationship(direction = Relationship.Direction.OUTGOING)
		private final List<Ticket> purchasedTickets;

		Client(List<Ticket> purchasedTickets) {
			this.purchasedTickets = purchasedTickets;
		}

		Long getId() {
			return id;
		}

		public List<Ticket> getPurchasedTickets() {
			return purchasedTickets;
		}

		@Override public String toString() {
			return "Client{" +
				   "purchasedTickets=" + purchasedTickets +
				   '}';
		}
	}

	/**
	 * This is used as a projection
	 */
	static class ClientStatistics {

		private final Long id;

		private final double totalSpend;

		private final double tickets;

		private final double spendPerTicket;

		public ClientStatistics(Long id, double totalSpend, double tickets, double spendPerTicket) {
			this.id = id;
			this.totalSpend = totalSpend;
			this.tickets = tickets;
			this.spendPerTicket = spendPerTicket;
		}

		@Override public String toString() {
			return "ClientStatistics{" +
				   "id=" + id +
				   ", totalSpend=" + totalSpend +
				   ", tickets=" + tickets +
				   ", spendPerTicket=" + spendPerTicket +
				   '}';
		}
	}

	@Node
	static class Ticket {

		/**
		 * Assigned id (an actual property and not the identy function).
		 */
		@Id
		private final Long id;

		@Property("datetime")
		private final ZonedDateTime purchasedAt;

		@Relationship("HAS_TICKETITEM")
		private final List<TicketItem> items;

		Ticket(Long id, ZonedDateTime purchasedAt, List<TicketItem> items) {
			this.id = id;
			this.purchasedAt = purchasedAt;
			this.items = items;
		}

		public Long getId() {
			return id;
		}

		public ZonedDateTime getPurchasedAt() {
			return purchasedAt;
		}

		public List<TicketItem> getItems() {
			return items;
		}

		@Override public String toString() {
			return "Ticket{" +
				   "purchasedAt=" + purchasedAt +
				   ", items=" + items +
				   '}';
		}
	}

	@Node
	static class TicketItem {

		/**
		 * Neo4j internally generated id.
		 */
		@Id @GeneratedValue
		private Long id;

		private final String product;

		private final Double netAmount;

		private final Long units;

		@Relationship("FOR_PRODUCT")
		private final Product relatedProduct;

		TicketItem(String product, Double netAmount, Long units, Product relatedProduct) {
			this.product = product;
			this.netAmount = netAmount;
			this.units = units;
			this.relatedProduct = relatedProduct;
		}

		public Long getId() {
			return id;
		}

		public String getProduct() {
			return product;
		}

		public Double getNetAmount() {
			return netAmount;
		}

		public Long getUnits() {
			return units;
		}

		public Product getRelatedProduct() {
			return relatedProduct;
		}

		@Override public String toString() {
			return "TicketItem{" +
				   "product='" + product + '\'' +
				   ", netAmount=" + netAmount +
				   ", units=" + units +
				   ", relatedProduct=" + relatedProduct +
				   '}';
		}
	}

	@Node
	static class Product {

		/**
		 * Neo4j internally generated id.
		 */
		@Id @GeneratedValue
		private Long id;

		private final String description;

		Product(String description) {
			this.description = description;
		}

		public Long getId() {
			return id;
		}

		public String getDescription() {
			return description;
		}

		@Override public String toString() {
			return "Product{" +
				   "description='" + description + '\'' +
				   '}';
		}
	}

	/**
	 * Also a DTO projection, but for products
	 */
	static class ProductStatistics {

		private final String description;

		private final double totalSpend;

		private final int purchases;

		ProductStatistics(String description, double totalSpend, int purchases) {
			this.description = description;
			this.totalSpend = totalSpend;
			this.purchases = purchases;
		}

		@Override public String toString() {
			return "ProductStatistics{" +
				   "description='" + description + '\'' +
				   ", totalSpend=" + totalSpend +
				   ", purchases=" + purchases +
				   '}';
		}
	}

	/**
	 * Please note how every delegated method is adding the database parameter to the context.
	 * This service is not used in the example, but added as an alternative.
	 * Care must be taken how to chain (flatMap and concatMap) queries.
	 */
	@Service
	@Transactional
	static class ClientService {
		private final ClientRepository repository;

		ClientService(ClientRepository repository) {
			this.repository = repository;
		}

		Flux<Client> findAll(Sort sort, String databaseName) {
			return inDatabase(databaseName, repository.findAll(sort));
		}

		Mono<Client> save(Client s, String databaseName) {
			return inDatabase(databaseName, repository.save(s));
		}

		Mono<Client> findById(Long aLong, String databaseName) {
			return inDatabase(databaseName, repository.findById(aLong));
		}

		Flux<ClientStatistics> getTop10BiggestSpendingClientsInMall(String databaseName) {
			return inDatabase(databaseName, repository.getTop10BiggestSpendingClients());
		}
	}
}
