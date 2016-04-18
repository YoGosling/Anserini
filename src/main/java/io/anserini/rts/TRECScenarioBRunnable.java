package io.anserini.rts;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import java.util.TimeZone;
import java.util.TimerTask;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.analysis.core.WhitespaceAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.Term;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.NumericRangeQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopScoreDocCollector;
import org.apache.lucene.search.TotalHitCountCollector;
import org.apache.lucene.search.similarities.DefaultSimilarity;
import org.apache.lucene.search.similarities.Similarity;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.MMapDirectory;
import org.apache.lucene.store.NoLockFactory;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
//import com.ibm.icu.util.TimeZone;

import io.anserini.index.IndexTweets.StatusField;
import twitter4j.JSONObject;

public class TRECScenarioBRunnable extends TimerTask {
	private static final JsonParser JSON_PARSER = new JsonParser();
	String mailList;
	String indexPath;
	// TRECSimilarity similarity=new TRECSimilarity();
	TitleExpansionSimilarity titleExpansionsimilarity = new TitleExpansionSimilarity();
	TitleCoordSimilarity titleCoordSimilarity = new TitleCoordSimilarity();
	String startTimestamp;
	TimeZone timeZone;
	DateFormat format;

	private static final Logger LOG = TRECIndexer.LOG;
	// LogManager.getLogger(TRECSearcher.class);

//	static TweetTopic[] topics;
	IndexWriter indexWriter = TRECIndexer.indexWriter;
	private IndexReader reader; // should it be static or one copy per
								// thread? to be answered
	private IndexReader newReader;
	// Set pushedTweets = new HashSet();
	HashMap<String, String> pushedTweets = new HashMap<String, String>();
	int dailylimit = 10;
	boolean shutDown = false; // if reached, shutDown=true, and the pushbroker
								// will sleep for the rest of the day
	String clientid;
	Client client = ClientBuilder.newClient();
	long interval;
	long dailyInterval = 24 * 3600000;
	String api_base;
	InterestProfile thisInterestProfile;
	int titleBoostFactor = 3;
	int expansionBoostFactor = 1;
	float duplicateThreshold = 0.6f;

	static public class InterestProfile {

		public String topicIndex;
		public String query;
		public ArrayList<String> expansion = new ArrayList<String>();
		public String titleQuery;
		public String titleExpansionQuery;
		public String expansionQuery;
		public int queryTokenCount;

		public String titleQueryString() {
			titleQuery = TRECTwokenizer.trecTokenizeQuery(query);
			queryTokenCount = query.split(" ").length;
			return titleQuery;
		}

		public String titleExpansionQueryString(int titleBoostFactor, int expansionBoostFactor) {
			String titleQuery = TRECTwokenizer.trecTokenizeQueryBoost(query, titleBoostFactor) + " ";
			expansionQuery = "";
			for (String expansionCluster : expansion) {
				expansionQuery = expansionQuery + "(" + expansionCluster + ")^"
						+ (new Integer(expansionBoostFactor).toString());
			}
			LOG.info(expansionQuery);
			// delete the endding whitespace
			return titleQuery + expansionQuery;

		}

		public InterestProfile(String topicIndex, String query, JsonArray expansion) {
			this.topicIndex = topicIndex;
			this.query = query;
			LOG.info("Query expansion terms " + expansion.toString());
			for (int i = 0; i < expansion.size(); i++) {
				JsonArray synonymArray = expansion.get(i).getAsJsonArray();
				ArrayList<String> synonymArrayList = new ArrayList<String>();
				for (int j = 0; j < synonymArray.size(); j++)
					synonymArrayList.add(synonymArray.get(j).toString());

				String concatenatedString = "";

				for (String t : synonymArrayList) {
					concatenatedString = concatenatedString + t + " ";
				}
				String thisExpansionCluster = "";

				for (String t : TRECTwokenizer.trecTokenizeQuery(concatenatedString).split(" ")) {
					thisExpansionCluster = thisExpansionCluster + t + " OR ";
				}

				this.expansion.add(thisExpansionCluster.substring(0, thisExpansionCluster.length() - 4));
			}
			LOG.info("The expansion term array parses as " + expansion.toString());
		}
	}


	public TRECScenarioBRunnable(String index, String interestProfilePath, String mailList,
			String startTimestamp) throws FileNotFoundException, IOException {

		LOG.info("Setting up TweetPusher Thread");
		this.indexPath = index;
		this.startTimestamp = startTimestamp;

		// this.indexWriter = indexWriter;
		// LOG.info("IndexWriter class: "+indexWriter.getClass());
		this.mailList = mailList;
		String JSONObjectString = "";
		try (BufferedReader br = new BufferedReader(new FileReader(interestProfilePath))) {
			String line = br.readLine();

			while (line != null) {
				JSONObjectString = JSONObjectString + line;
				line = br.readLine();
			}
		}
		JsonObject interestProfileObject = (JsonObject) JSON_PARSER.parse(JSONObjectString);
		thisInterestProfile = new InterestProfile(interestProfileObject.get("index").toString(),
				interestProfileObject.get("query").toString(), interestProfileObject.getAsJsonArray("expansion"));
		LOG.info("Set up TweetPusher Thread");
		timeZone = TimeZone.getTimeZone("UTC");
		format = new SimpleDateFormat("E dd MMM yyyy HH:mm:ss zz");
		format.setTimeZone(TimeZone.getTimeZone("UTC"));

	}

	public class ScoreDocComparator implements Comparator<ScoreDocTimestamp> {

		@Override
		public int compare(ScoreDocTimestamp o1, ScoreDocTimestamp o2) {
			Float f1 = new Float(o1.score);
			// TODO Auto-generated method stub
			if (f1 != o2.score) {
				return (-1) * f1.compareTo(new Float(o2.score));
			} else {
				Long l1 = new Long(o1.timestamp);
				return (-1) * (l1.compareTo(new Long(o2.timestamp)));
			}
		}
	}

	public boolean isDuplicate(String whiteSpaceTokenizedText) {
		Set<String> thisTokens = new HashSet<String>(Arrays.asList(whiteSpaceTokenizedText.split(" ")));
		LOG.info("thisToken " + thisTokens);
		for (String previousWhiteSpaceTokenizedText : pushedTweets.values()) {
			Set<String> previousTokens = new HashSet<String>(
					Arrays.asList((previousWhiteSpaceTokenizedText).split(" ")));
			Set intersectionTokens = new HashSet(thisTokens);
			intersectionTokens.retainAll(previousTokens);
			if ((intersectionTokens.size() * 1.0 / thisTokens.size()) > duplicateThreshold) {
				LOG.info("Duplicate with previous intersectionTokens " + intersectionTokens);
				LOG.info("Will discard this old/redundent one");
				return true;
			}

		}
		return false;
	}

	@SuppressWarnings("deprecation")
	@Override
	public void run() {
		LOG.info("Running TweetPusher Thread");
		try {

			pushedTweets.clear();

			Query q = new QueryParser(TRECIndexerRunnable.StatusField.TEXT.name, TRECIndexer.ANALYZER)
					.parse(thisInterestProfile.titleQueryString());

			LOG.info("Parsed query " + q.getClass() + " looks like " + q.toString() + " " + q.getClass());

			System.out.println(indexPath);
			reader = DirectoryReader.open(FSDirectory.open(new File(indexPath).toPath()));

			newReader = DirectoryReader.openIfChanged((DirectoryReader) reader);
			if (newReader != null) {
				reader.close();
				reader = newReader;
			}
			IndexSearcher searcher = new IndexSearcher(reader);
			LOG.info("default similarity class:" + searcher.getDefaultSimilarity());

			searcher.setSimilarity(titleCoordSimilarity);
			// TODO
			TotalHitCountCollector totalHitCollector = new TotalHitCountCollector();
			searcher.search(q, totalHitCollector);

			if (totalHitCollector.getTotalHits() > 0) {
				TopScoreDocCollector coordCollector = TopScoreDocCollector
						.create(Math.max(0, totalHitCollector.getTotalHits()));
				searcher.search(q, coordCollector);
				ScoreDoc[] coordHits = coordCollector.topDocs().scoreDocs;
				HashMap<Integer, Float> coordHMap = new HashMap<Integer, Float>();
				for (ScoreDoc s : coordHits) {
					coordHMap.put(s.doc, s.score);
				}

				LOG.info("title coordinate similarity has hits:" + totalHitCollector.getTotalHits());

				Query fullQuery = new QueryParser(TRECIndexerRunnable.StatusField.TEXT.name, TRECIndexer.ANALYZER)
						.parse(thisInterestProfile.titleExpansionQueryString(titleBoostFactor, expansionBoostFactor));

				BooleanQuery finalQuery = new BooleanQuery();
				finalQuery.add(fullQuery, BooleanClause.Occur.MUST);
				// finalQuery.add(q,BooleanClause.Occur.MUST);
				Query tweetTimeRangeQuery = NumericRangeQuery.newLongRange(StatusField.EPOCH.name,
						(long) (Calendar.getInstance().getTimeInMillis() - dailyInterval) / 1000,
						(long) Calendar.getInstance().getTimeInMillis() / 1000, true, true);

				// must satisfy the time window but not participate into
				// scoring
				finalQuery.add(tweetTimeRangeQuery, BooleanClause.Occur.FILTER);

				LOG.info("Parsed fullQuery " + fullQuery.getClass() + " looks like " + fullQuery.toString() + " "
						+ fullQuery.getClass());
				LOG.info("Parsed finalQuery " + finalQuery.getClass() + " looks like " + finalQuery.toString() + " "
						+ finalQuery.getClass());
				searcher.setSimilarity(titleExpansionsimilarity);

				totalHitCollector = new TotalHitCountCollector();

				searcher.search(finalQuery, totalHitCollector);

				if (totalHitCollector.getTotalHits() > 0) {
					TopScoreDocCollector collector = TopScoreDocCollector
							.create(Math.max(0, totalHitCollector.getTotalHits()));
					searcher.search(finalQuery, collector);
					ScoreDoc[] hits = collector.topDocs().scoreDocs;
					LOG.info("title expansion similarity has XXX hits:" + totalHitCollector.getTotalHits());

					ArrayList<ScoreDocTimestamp> finalHits = new ArrayList<ScoreDocTimestamp>();

					for (int j = 0; j < hits.length; ++j) {
						int docId = hits[j].doc;
						float docScore = hits[j].score;
						Document fullDocument = searcher.doc(docId);
						long timestamp = Long.parseLong(fullDocument.get(TRECIndexerRunnable.StatusField.EPOCH.name));
						if (coordHMap.containsKey(docId)) {
							finalHits.add(new ScoreDocTimestamp(docId, docScore * coordHMap.get(docId), timestamp,
									fullDocument));
						}
					}

					Collections.sort(finalHits, new ScoreDocComparator());
					LOG.info("Hit " + finalHits.size() + " documents");
					if (0 != finalHits.size()) {
						LOG.info("_______________________________________________");
						LOG.info("Quering:" + fullQuery.toString() + ", Found " + finalHits.size()
								+ " hits (including old, only push new)");
					}

					ArrayList<String> tweetList = new ArrayList<String>();
					ArrayList<String> userNameList = new ArrayList<String>();
					ArrayList<String> userScreenNameList = new ArrayList<String>();
					ArrayList<String> userImageProfileURLList = new ArrayList<String>();
					ArrayList<String> textList = new ArrayList<String>();
					ArrayList<String> epochList = new ArrayList<String>();

					for (int j = 0; j < finalHits.size(); ++j) {
						int docId = finalHits.get(j).doc;
						Document d = finalHits.get(j).fullDocument;

						if (pushedTweets.size() < dailylimit
								&& !pushedTweets.containsKey(d.get(TRECIndexerRunnable.StatusField.ID.name))
								&& !isDuplicate(d.get(TRECIndexerRunnable.StatusField.TEXT.name))
								&& finalHits.get(j).score >= thisInterestProfile.queryTokenCount * 2) {
							LOG.info(finalHits.get(j).score + " " + thisInterestProfile.queryTokenCount);
							tweetList.add(d.get(TRECIndexerRunnable.StatusField.ID.name));
							userNameList.add(d.get(TRECIndexerRunnable.StatusField.NAME.name));
							userScreenNameList.add(d.get(TRECIndexerRunnable.StatusField.SCREEN_NAME.name));
							userImageProfileURLList.add(d.get(TRECIndexerRunnable.StatusField.PROFILE_IMAGE_URL.name));
							textList.add(d.get(TRECIndexerRunnable.StatusField.RAW_TEXT.name));

							Date date = new Date(
									Long.parseLong(d.get(TRECIndexerRunnable.StatusField.EPOCH.name) + "000"));
							String formatted = format.format(date);

							System.out.println(date.toString() + " " + formatted);
							epochList.add(formatted);

							LOG.info("Tweet ID:" + String.valueOf(d.get(TRECIndexerRunnable.StatusField.ID.name)));
							LOG.info("Tweet text:" + d.get(TRECIndexerRunnable.StatusField.RAW_TEXT.name));
							LOG.info("Tweet epoch:" + d.get(TRECIndexerRunnable.StatusField.EPOCH.name));
							pushedTweets.put(d.get(TRECIndexerRunnable.StatusField.ID.name),
									d.get(TRECIndexerRunnable.StatusField.TEXT.name));

						}
					}
					if (tweetList.size() > 0)
						SendAttachmentInEmail.sendDigestBlockquote(mailList, tweetList, userNameList,
								userScreenNameList, userImageProfileURLList, textList, epochList, startTimestamp,
								"ScenarioB " + thisInterestProfile.topicIndex + ":" + thisInterestProfile.query+" #"+(pushedTweets.size()-tweetList.size()+1)+"-"+pushedTweets.size());

				}
			} else {
				LOG.info("For this iteration, no single tweet hit even only the title field");
			}
			reader.close();

		}

		catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ParseException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}

	}
}
