package io.anserini.rts;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.StringReader;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.TimeZone;
import java.util.Timer;
import java.util.TimerTask;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.core.WhitespaceAnalyzer;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.MMapDirectory;

import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHandler;
import org.eclipse.jetty.server.handler.ResourceHandler;
import org.eclipse.jetty.server.handler.HandlerList;

import io.anserini.index.twitter.TweetAnalyzer;
import twitter4j.JSONException;

import org.eclipse.jetty.servlet.ServletHolder;
import org.glassfish.jersey.servlet.ServletContainer;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.type.TypeFactory;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

public class TRECSearcher {
	public static final Logger LOG = LogManager.getLogger(TRECSearcher.class);

	private static final String HOST_OPTION = "host";
	private static final String INDEX_OPTION = "index";
	private static final String PORT_OPTION = "port";
	private static final String GROUPID_OPTION = "groupid";
	static String api_base;
	static TRECTopic[] topics;

	static Client client = ClientBuilder.newClient();
	static String clientid;
	static String groupid;

	static String interestProfilePath;
	static String[] mailList;

	public static Directory index;
	public static String indexName;
	public static IndexWriter indexWriter;
	public static TRECIndexerRunnable its;
	public static Server server;

	public static final Analyzer ANALYZER = new WhitespaceAnalyzer();
	public static boolean isServerTerminated = false;

	static long minuteInterval = 60000;
	static long dailyInterval = 24 * 3600000;

	public TRECSearcher(String dir) throws IOException {
		indexName = dir;

		FileUtils.deleteDirectory(new File(dir));

		index = new MMapDirectory(Paths.get(dir));
		IndexWriterConfig config = new IndexWriterConfig(ANALYZER);
		config.setSimilarity(new TRECSimilarity());
		indexWriter = new IndexWriter(index, config);

	}

	public TRECSearcher() throws IOException {
	}

	public void close() throws IOException {
		indexWriter.close();
	}

	/* First stage: client registers from broker and gets client id */
	public void register() throws JsonProcessingException, IOException, JSONException {
		WebTarget webTarget = client.target(api_base + "register/system");

		Response postResponse = webTarget.request(MediaType.APPLICATION_JSON)
				.post(Entity.entity(new String("{\"groupid\":\"" + groupid + "\"}"), MediaType.APPLICATION_JSON));
		LOG.info("Registrer status " + postResponse.getStatus());

		if (postResponse.getStatus() == 200) {
			String jsonString = postResponse.readEntity(String.class);
			JsonNode rootNode = new ObjectMapper().readTree(new StringReader(jsonString));
			clientid = rootNode.get("clientid").asText();
			LOG.info("Register success, clientid is " + clientid);
		} else
			try {
				throw new RegisterException("Register failed to register with this groupid");
			} catch (RegisterException e) {
				System.out.println(postResponse.getStatus());
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

	}

	@JsonIgnoreProperties(ignoreUnknown = true)
	static public class TRECTopic {
		@JsonProperty("topid")
		public String topid;
		@JsonProperty("query")
		public String query;

		@JsonCreator
		public TRECTopic(@JsonProperty("topid") String topicID, @JsonProperty("query") String query) {
			super();
			this.topid = topicID;
			this.query = query;
		}
	}

	/* Second stage: client gets topics from broker */
	public void getTopic() throws JsonParseException, JsonMappingException, IOException, JSONException {
		/*
		 * topics Json format:
		 * [{"topid":"test1","query":"birthday"},{"topid":"test2","query":
		 * "batman"},{"topid":"test3","query":"star wars"}]
		 */
		WebTarget webTarget = client.target(api_base + "topics/" + clientid);

		Response postResponse = webTarget.request(MediaType.APPLICATION_JSON).get();

		if (postResponse.getStatus() == 200) {
			LOG.info("Retrieve topics success");
			String jsonString = postResponse.readEntity(String.class);
			ObjectMapper mapper = new ObjectMapper();
			topics = mapper.readValue(jsonString, TypeFactory.defaultInstance().constructArrayType(TRECTopic.class));

			File file = new File("src/main/java/io/anserini/rts/public/TREC2016Profiles");
			boolean isDirectoryCreated = file.mkdir();
			if (isDirectoryCreated) {
				LOG.info("Interest profile directory successfully made");
			} else {
				FileUtils.deleteDirectory(file);
				file.mkdir();
				LOG.info("Interest profile directory deleted and made");
			}

			for (int i = 0; i < topics.length; i++) {

				JsonObject obj = new JsonObject();
				obj.addProperty("index", topics[i].topid);
				obj.addProperty("query", topics[i].query);
				obj.add("expansion", new JsonArray());

				// try-with-resources statement based on post comment below :)
				try (FileWriter topicFile = new FileWriter(
						"src/main/java/io/anserini/rts/public/TREC2016Profiles/" + topics[i].topid + ".json")) {
					topicFile.write(obj.toString());
					LOG.info("Successfully wrote topic interest profile to disk...Topic " + topics[i].topid + ": "
							+ topics[i].query);
				}
			}
		}

	}

	class RegisterException extends Exception {
		public RegisterException(String msg) {
			super(msg);
		}
	}

	public static void main(String[] args) throws Exception {
		Options options = new Options();
		options.addOption(HOST_OPTION, true, "hostname");
		options.addOption(INDEX_OPTION, true, "index path");
		options.addOption(PORT_OPTION, true, "port");
		options.addOption(GROUPID_OPTION, true, "groupid");

		options.addOption("broker", true, "broker");
		options.addOption("mailList", true, "mailList");

		CommandLine cmdline = null;
		CommandLineParser parser = new GnuParser();
		try {
			cmdline = parser.parse(options, args);
		} catch (org.apache.commons.cli.ParseException e) {
			System.err.println("Error parsing command line: " + e.getMessage());
			System.exit(-1);
		}

		if (!cmdline.hasOption(INDEX_OPTION)) {
			HelpFormatter formatter = new HelpFormatter();
			formatter.printHelp(TRECSearcher.class.getName(), options);
			System.exit(-1);
		}

		String host = cmdline.getOptionValue(HOST_OPTION);
		groupid = cmdline.getOptionValue(GROUPID_OPTION);
		int port = cmdline.hasOption(PORT_OPTION) ? Integer.parseInt(cmdline.getOptionValue(PORT_OPTION)) : 8080;
		api_base = new String("http://" + host + ":" + port + "/");

		TRECSearcher nrtsearch = new TRECSearcher(cmdline.getOptionValue(INDEX_OPTION));
		nrtsearch.register();
		nrtsearch.getTopic();

		LOG.info("Starting TRECStreamIndexer");

		its = new TRECIndexerRunnable(indexWriter);
		Thread itsThread = new Thread(its);
		itsThread.start();

		LOG.info(TRECSearcher.its.startTime);
		LOG.info("Starting HTTP server on port 8080");

		HandlerList mainHandler = new HandlerList();

		server = new Server(8080);

		ResourceHandler resource_handler = new ResourceHandler();

		resource_handler.setResourceBase("src/main/java/io/anserini/rts/public");
		resource_handler.setWelcomeFiles(new String[] { "index.html" });

		ServletContextHandler handler = new ServletContextHandler(ServletContextHandler.SESSIONS);
		handler.setContextPath("/");
		ServletHolder jerseyServlet = new ServletHolder(ServletContainer.class);
		jerseyServlet.setInitParameter("jersey.config.server.provider.classnames",
				TRECIndexerServerAPI.class.getCanonicalName());
		handler.addServlet(jerseyServlet, "/*");

		mainHandler.addHandler(resource_handler);
		mainHandler.addHandler(handler);
		server.setHandler(mainHandler);
		try {
			server.start();
			LOG.info("Accepting connections on port 8080");
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		itsThread.join();

		String mailList = cmdline.hasOption("mailList") ? cmdline.getOptionValue("mailList")
				: new String("445232908@qq.com");
		@SuppressWarnings("deprecation")
		Calendar now = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
		String startTimestamp = "run since " + now.getTime().toString();
		LOG.info("Get current epoch" + Calendar.getInstance().getTimeInMillis());
		ArrayList<TimerTask> threadList = new ArrayList<TimerTask>();
		ArrayList<Timer> timerList = new ArrayList<Timer>();

		now = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
		Calendar tomorrow = Calendar.getInstance();
		tomorrow.set(Calendar.HOUR, 0);
		tomorrow.set(Calendar.MINUTE, 0);
		tomorrow.set(Calendar.SECOND, 0);
		tomorrow.set(Calendar.AM_PM, Calendar.AM);
		tomorrow.set(Calendar.DAY_OF_YEAR, now.get(Calendar.DAY_OF_YEAR) + 1);
		tomorrow.setTimeZone(TimeZone.getTimeZone("UTC"));

		for (TRECTopic topic : topics) {
			Timer timer = new Timer();
			if (cmdline.hasOption("mailList")) {
				TimerTask tasknew = new TRECScenarioARunnable(indexName,
						"src/main/java/io/anserini/rts/public/TREC2016Profiles/" + topic.topid + ".json", mailList,
						startTimestamp);

				timer.scheduleAtFixedRate(tasknew, 0, minuteInterval);
				threadList.add(tasknew);
				timerList.add(timer);
			} else if (cmdline.getOptionValue("broker").equals("yes")) {
				TimerTask tasknew = new TRECScenarioARunnable(indexName,
						"src/main/java/io/anserini/rts/public/TREC2016Profiles/" + topic.topid + ".json",
						api_base + "tweet/" + topic.topid + "/:tweetid/" + clientid);

				timer.scheduleAtFixedRate(tasknew, 30000, minuteInterval);
				threadList.add(tasknew);
				timerList.add(timer);
			}
		}

		for (TRECTopic topic : topics) {
			Timer timer = new Timer();
			if (cmdline.hasOption("mailList")) {
				TimerTask tasknew = new TRECScenarioBRunnable(indexName,
						"src/main/java/io/anserini/rts/public/TREC2016Profiles/" + topic.topid + ".json", mailList,
						startTimestamp);
				LOG.info(tomorrow.getTimeInMillis() + " " + now.getTimeInMillis());
				timer.scheduleAtFixedRate(tasknew, (long) (tomorrow.getTimeInMillis() - now.getTimeInMillis()),
						dailyInterval);
				threadList.add(tasknew);
				timerList.add(timer);
			} else if (cmdline.getOptionValue("broker").equals("yes")) {
				TimerTask tasknew = new TRECScenarioBRunnable(indexName,
						"src/main/java/io/anserini/rts/public/TREC2016Profiles/" + topic.topid + ".json",
						api_base + "tweets/" + topic.topid + "/" + clientid);
				LOG.info(tomorrow.getTimeInMillis() + " " + now.getTimeInMillis());
				timer.scheduleAtFixedRate(tasknew, (long) (tomorrow.getTimeInMillis() - now.getTimeInMillis()),
						dailyInterval);
				threadList.add(tasknew);
				timerList.add(timer);

			}
		}

		LOG.info("TRECQueryListernerThread started");

		while (!itsThread.isAlive() && !isServerTerminated) {
			Thread.sleep(3000);
			System.out.println("In Main, isServerTerminated=false");
		}
		System.out.println("In Main, isServerTerminated=true");
		server.stop();
		server.join();

		nrtsearch.close();
	}
}
