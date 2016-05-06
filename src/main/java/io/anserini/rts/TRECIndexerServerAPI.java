package io.anserini.rts;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopScoreDocCollector;


@Path("/api")
public class TRECIndexerServerAPI {
	
	private static final long serialVersionUID = 1L;
	private IndexReader reader;

	static class SearchAPIQuery {
		private String query;
		private int count;

		public SearchAPIQuery() {
			count = 20;
		}

		public int getCount() {
			return count;
		}

		public void setCount(int count) {
			this.count = count;
		}

		public SearchAPIQuery(String query, int count) {

			this.query = query;
			this.count = count;
		}

		public String getQuery() {
			return query;
		}

		public void setQuery(String query) {
			this.query = query;
		}
	}

	static class SearchResult {
		String docid;

		public SearchResult() {
		}

		public String getDocid() {

			return docid;
		}

		public void setDocid(String docid) {
			this.docid = docid;
		}

		public SearchResult(String docid) {
			this.docid = docid;
		}
	}

	static class indexStatus {
		String indexName;
		String indexPath;
		int tweetsIndexed;
		String startTime;
		String stopTime;

		public indexStatus() {
		}

		public String getIndexName() {
			return indexName;
		}

		public void setIndexName(String indexName) {
			this.indexName = indexName;
		}

		public String getIndexPath() {
			return indexPath;
		}

		public void setIndexPath(String indexPath) {
			this.indexPath = indexPath;
		}

		public int getTweetsIndexed() {
			return tweetsIndexed;
		}

		public void setTweetsIndexed(int tweetsIndexed) {
			this.tweetsIndexed = tweetsIndexed;
		}

		public String getStartTime(){
			return startTime;
		}
		
		public void setStartTime(String startTime){
			this.startTime=startTime;
		}
		
		public String getStopTime(){
			return stopTime;
		}
		
		public void setStopTime(String stopTime){
			this.stopTime=stopTime;
		}
		
		public indexStatus(String indexName, String indexPath, int tweetsIndexed, String startTime, String stopTime) {

			this.indexName = indexName;
			this.indexPath = indexPath;
			this.tweetsIndexed = tweetsIndexed;
			this.startTime = startTime;
			this.stopTime = stopTime;
		}

		public indexStatus(String indexName, String indexPath, int tweetsIndexed) {
			this.indexName = indexName;
			this.indexPath = indexPath;
			this.tweetsIndexed = tweetsIndexed;
		}
	}

	@POST
	@Path("search")
	@Produces(MediaType.APPLICATION_JSON)
	public List<SearchResult> search(SearchAPIQuery query) {
		try {
			Query q = new QueryParser(TRECIndexerRunnable.StatusField.TEXT.name,TRECIndexer.ANALYZER)
					.parse(query.getQuery());
			try {
				reader = DirectoryReader.open(TRECIndexer.indexWriter, true);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			IndexReader newReader = DirectoryReader.openIfChanged((DirectoryReader) reader, TRECIndexer.indexWriter,
					true);
			if (newReader != null) {
				reader.close();
				reader = newReader;
			}
			IndexSearcher searcher = new IndexSearcher(reader);

			int topN = query.getCount();
			TopScoreDocCollector collector = TopScoreDocCollector.create(topN);
			searcher.search(q, collector);
			ScoreDoc[] hits = collector.topDocs().scoreDocs;
			List<SearchResult> resultHits = new ArrayList<>();

			for (int i = 0; i < hits.length && i < topN; ++i) {
				int docId = hits[i].doc;
				Document d = searcher.doc(docId);
				resultHits.add(new SearchResult(String.valueOf(d.get(TRECIndexerRunnable.StatusField.ID.name))));
			}
			return resultHits;
		} catch (Exception e) {
			e.printStackTrace();
			return new ArrayList<>();
		}

	}

	@GET
	@Path("/index")
	@Produces(MediaType.APPLICATION_JSON)
	public indexStatus getTrackInJSON() {
		indexStatus thisIndex = new indexStatus();
		thisIndex.setIndexName(TRECIndexer.indexName.toString());
		thisIndex.setIndexPath(TRECIndexer.index.toString());
		thisIndex.setTweetsIndexed(TRECIndexerRunnable.tweetCount);
		thisIndex.setStartTime(TRECIndexer.its.startTime);
		thisIndex.setStopTime(TRECIndexer.its.endTime);
		return thisIndex;

	}

	@GET
	@Path("/stop")
	@Produces(MediaType.APPLICATION_JSON)
	public indexStatus stopIndexing() throws Exception {

		System.out.println("Gonna stop sampling, how many tweets received? see above");

		// stop in this manner
		TRECIndexer.its.twitterStream.shutdown();
		TRECIndexer.its.terminate();

		indexStatus thisIndex = new indexStatus();
		thisIndex.setIndexName(TRECIndexer.indexName.toString());
		thisIndex.setIndexPath(TRECIndexer.index.toString());
		thisIndex.setTweetsIndexed(TRECIndexerRunnable.tweetCount);
		thisIndex.setStartTime(TRECIndexer.its.startTime);
		thisIndex.setStopTime(TRECIndexer.its.endTime);
		return thisIndex;

	}

	@GET
	@Path("/terminate")
	@Produces(MediaType.APPLICATION_JSON)
	public indexStatus terminateIndexing() throws Exception {

		System.out.println("Gonna stop the process");
		System.out.println("Main thread Sleeping...");
		TRECIndexer.isServerTerminated = true;
		System.out.println("In API, isServerTerminated=true");
		indexStatus thisIndex = new indexStatus("Indexing stopped!", "Indexing stopped!", 0);
		return thisIndex;

	}

}
