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
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.MMapDirectory;

import twitter4j.JSONException;

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
import com.google.gson.JsonObject;

public class TRECSearcher {
	public static final Logger LOG = LogManager.getLogger(TRECSearcher.class);

	private static final String HOST_OPTION = "host";
	private static final String INDEX_OPTION = "index";
	private static final String PORT_OPTION = "port";
	private static final String GROUPID_OPTION = "groupid";
	private static final String interestProfilePath = "src/main/java/io/anserini/rts/public/TREC2016Profiles/";

	private static String api_base;
	private static TRECTopic[] topics;

	private static final Client client = ClientBuilder.newClient();
	private static String clientid;
	private static String groupid;

	public static Directory index;
	public static IndexWriter indexWriter;
	public static String indexName;
	public static TRECIndexerRunnable its;
	public static final Analyzer ANALYZER = new WhitespaceAnalyzer();

	private static long minuteInterval = 60 * 1000;
	private static long dailyInterval = 24 * 60 * 60 * 1000;

	public TRECSearcher(String dir) throws IOException {
		FileUtils.deleteDirectory(new File(dir));
		index = new MMapDirectory(Paths.get(dir));
		indexName = dir;
		IndexWriterConfig config = new IndexWriterConfig(ANALYZER);
		indexWriter = new IndexWriter(index, config);
	}

	public void close() throws IOException {
		indexWriter.close();
	}

	class ConnectBrokerAPIException extends Exception {
		/**
		 * 
		 */
		private static final long serialVersionUID = 1L;

		public ConnectBrokerAPIException(String msg) {
			super(msg);
		}
	}

	/* First stage: client registers from broker and gets client id */
	public void register() throws JsonProcessingException, IOException, JSONException {
		WebTarget webTarget = client.target(api_base + "register/system");
		Response postResponse = webTarget.request(MediaType.APPLICATION_JSON)
				.post(Entity.entity(new String("{\"groupid\":\"" + groupid + "\"}"), MediaType.APPLICATION_JSON));
		LOG.info("Register status " + postResponse.getStatus());
		if (postResponse.getStatus() == 200) {
			String jsonString = postResponse.readEntity(String.class);
			JsonNode rootNode = new ObjectMapper().readTree(new StringReader(jsonString));
			clientid = rootNode.get("clientid").asText();
			LOG.info("Register success with clientid " + clientid);
		} else
			try {
				throw new ConnectBrokerAPIException("Register failed with the groupid " + groupid);
			} catch (ConnectBrokerAPIException e) {
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
		WebTarget webTarget = client.target(api_base + "topics/" + clientid);
		Response postResponse = webTarget.request(MediaType.APPLICATION_JSON).get();

		if (postResponse.getStatus() == 200) {
			LOG.info("Get topics success");
			String jsonString = postResponse.readEntity(String.class);
			ObjectMapper mapper = new ObjectMapper();
			topics = mapper.readValue(jsonString, TypeFactory.defaultInstance().constructArrayType(TRECTopic.class));

			File file = new File(interestProfilePath);
			boolean isDirectoryCreated = file.mkdir();
			if (isDirectoryCreated) {
				LOG.info("Interest profile directory successfully made");
			} else {
				FileUtils.deleteDirectory(file);
				file.mkdir();
				LOG.info("Interest profile directory successfully covered");
			}
			for (int i = 0; i < topics.length; i++) {

				JsonObject obj = new JsonObject();
				obj.addProperty("index", topics[i].topid);
				obj.addProperty("query", topics[i].query);
				obj.add("expansion", new JsonArray());

				try (FileWriter topicFile = new FileWriter(interestProfilePath + topics[i].topid + ".json")) {
					topicFile.write(obj.toString());
					LOG.info("Successfully wrote interest profile " + topics[i].topid + " to disk.");
				}
			}
		} else
			try {
				throw new ConnectBrokerAPIException("Get topics failed.");
			} catch (ConnectBrokerAPIException e) {
				System.out.println(postResponse.getStatus());
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

	}

	public static void main(String[] args) throws Exception {
		Options options = new Options();
		options.addOption(HOST_OPTION, true, "host");
		options.addOption(INDEX_OPTION, true, "index path");
		options.addOption(PORT_OPTION, true, "port");
		options.addOption(GROUPID_OPTION, true, "groupid");

		CommandLine cmdline = null;
		CommandLineParser parser = new GnuParser();
		try {
			cmdline = parser.parse(options, args);
		} catch (org.apache.commons.cli.ParseException e) {
			System.err.println("Error parsing command line: " + e.getMessage());
			System.exit(-1);
		}

		if (!cmdline.hasOption(HOST_OPTION) || !cmdline.hasOption(INDEX_OPTION) || !cmdline.hasOption(PORT_OPTION)
				|| !cmdline.hasOption(GROUPID_OPTION)) {
			HelpFormatter formatter = new HelpFormatter();
			formatter.printHelp(TRECSearcher.class.getName(), options);
			System.exit(-1);
		}

		String host = cmdline.getOptionValue(HOST_OPTION);
		groupid = cmdline.getOptionValue(GROUPID_OPTION);
		int port = Integer.parseInt(cmdline.getOptionValue(PORT_OPTION));
		api_base = new String("http://" + host + ":" + port + "/");
		TRECSearcher rtsSearch = new TRECSearcher(cmdline.getOptionValue(INDEX_OPTION));
		rtsSearch.register();
		rtsSearch.getTopic();

		its = new TRECIndexerRunnable(indexWriter);
		Thread itsThread = new Thread(its);
		itsThread.start();

		Calendar now = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
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
			TimerTask tasknew = new TRECScenarioRunnable(indexName, interestProfilePath + topic.topid + ".json",
					api_base + "tweet/" + topic.topid + "/:tweetid/" + clientid, "A");

			// Schedule scenario A search task every minute Interval
			// At the first time, there's a 30000 milliseconds delay for the
			// delay in connecting Twitter Streaming API
			timer.scheduleAtFixedRate(tasknew, 30000, minuteInterval);
			threadList.add(tasknew);
			timerList.add(timer);
		}

		for (TRECTopic topic : topics) {
			Timer timer = new Timer();
			TimerTask tasknew = new TRECScenarioRunnable(indexName, interestProfilePath + topic.topid + ".json",
					api_base + "tweets/" + topic.topid + "/" + clientid, "B");
			LOG.info(tomorrow.getTimeInMillis() + " " + now.getTimeInMillis());

			// Schedule scenario B search task every day at 0'00'01
			// The 1000 milliseconds delay is to ensure that the search action
			// lies exactly at the new day, as long as 1000 milliseconds delay
			// will not discount the reward.
			// At the first time, there's a delay to wait till a new day.
			timer.scheduleAtFixedRate(tasknew, (long) (tomorrow.getTimeInMillis() - now.getTimeInMillis() + 1000),
					dailyInterval);
			threadList.add(tasknew);
			timerList.add(timer);
		}

		itsThread.join();
	}
}
