package io.anserini.rts;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;

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

import org.eclipse.jetty.servlet.ServletHolder;
import org.glassfish.jersey.servlet.ServletContainer;

public class TRECIndexer {
	public static final Logger LOG = LogManager.getLogger(TRECIndexer.class);

	private static final String INDEX_OPTION = "index";

	private static final String PORT_OPTION = "port";

	static String interestProfilePath;
	static String[] mailList;

	public static Directory index;
	public static String indexName;
	public static IndexWriter indexWriter;
	public static TRECIndexerRunnable its;
	public static Server server;

	public static final Analyzer ANALYZER = new WhitespaceAnalyzer();
	public static boolean isServerTerminated = false;

	public TRECIndexer(String dir) throws IOException {
		indexName = dir;

		FileUtils.deleteDirectory(new File(dir));

		index = new MMapDirectory(Paths.get(dir));
		IndexWriterConfig config = new IndexWriterConfig(ANALYZER);
		config.setSimilarity(new TRECSimilarity());
		indexWriter = new IndexWriter(index, config);

	}

	public TRECIndexer() throws IOException {
	}

	public void close() throws IOException {
		indexWriter.close();
	}

	public static void main(String[] args) throws Exception {
		Options options = new Options();
		options.addOption(INDEX_OPTION, true, "index path");

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
			formatter.printHelp(TRECIndexer.class.getName(), options);
			System.exit(-1);
		}

		TRECIndexer nrtsearch = new TRECIndexer(cmdline.getOptionValue(INDEX_OPTION));

		LOG.info("Starting TRECStreamIndexer");

		its = new TRECIndexerRunnable(indexWriter);
		Thread itsThread = new Thread(its);
		itsThread.start();

		LOG.info(TRECIndexer.its.startTime);
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

		while (!itsThread.isAlive() && !isServerTerminated) {
			Thread.sleep(3000);
			System.out.println(its.isRunning + "In Main, isServerTerminated=false");
		}
		System.out.println("In Main, isServerTerminated=true");
		server.stop();
		server.join();

		nrtsearch.close();
	}
}
