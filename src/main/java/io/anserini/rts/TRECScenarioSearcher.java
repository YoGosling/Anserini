package io.anserini.rts;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.TimeZone;
import java.util.Timer;
import java.util.TimerTask;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.Options;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class TRECScenarioSearcher {
	static long minuteInterval = 60000;
	static long dailyInterval = 24 * 3600000;
	final static Logger LOG = LogManager.getLogger(TRECScenarioSearcher.class);

	public static void main(String[] args) throws FileNotFoundException, IOException, InterruptedException {

		// TODO Auto-generated method stub
		Options options = new Options();
		// options.addOption("interestProfilePath", false,
		// "interestProfilePath");
		options.addOption("mailList", true, "mailList");
		options.addOption("index", true, "indexPath");
		options.addOption("startTimestamp", true, "timestamp");
		options.addOption("startTopic", true, "startTopic");
		options.addOption("stopTopic", true, "stopTopic");
		options.addOption("topicSequence", true, "topicSequence");
		options.addOption("scenario", true, "scenario");

		CommandLine cmdline = null;
		CommandLineParser parser = new GnuParser();
		try {
			cmdline = parser.parse(options, args);
		} catch (org.apache.commons.cli.ParseException e) {
			System.err.println("Error parsing command line: " + e.getMessage());
			System.exit(-1);
		}
		LOG.info(cmdline.getOptionValue("mailList"));
		String mailList = cmdline.hasOption("mailList") ? cmdline.getOptionValue("mailList")
				: new String("445232908@qq.com");
		LOG.info(cmdline.getOptionValue("mailList"));
		LOG.info(cmdline.getOptionValue("interestProfilePath"));
		String index = cmdline.getOptionValue("index");
		ArrayList<Integer> topicList = new ArrayList<Integer>();
		if (cmdline.hasOption("startTopic")) {
			Integer startTopic = Integer.valueOf(cmdline.getOptionValue("startTopic"));
			Integer stopTopic = Integer.valueOf(cmdline.getOptionValue("stopTopic"));
			for (int i = startTopic; i <= stopTopic; i++)
				topicList.add(i);
		}
		if (cmdline.hasOption("topicSequence")) {
			String topicSequence = cmdline.getOptionValue("topicSequence");
			LOG.info(topicSequence+" "+cmdline.getOptionValue("topicSequence"));
			for (String s : topicSequence.split(":")) {
				topicList.add(Integer.valueOf(s));
			}
		}

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

		if (cmdline.getOptionValue("scenario").equals("A")) {
			for (int i : topicList) {
				Timer timer = new Timer();
				TimerTask tasknew = new TRECScenarioARunnable(index,
						"src/main/java/io/anserini/rts/public/TREC2015Profiles/MB" + (new String().valueOf(i))
								+ ".json",
						mailList, startTimestamp);
				timer.scheduleAtFixedRate(tasknew, 0, minuteInterval);
				threadList.add(tasknew);
				timerList.add(timer);
			}
		} else if (cmdline.getOptionValue("scenario").equals("B")) {
			for (int i : topicList) {
				Timer timer = new Timer();
				TimerTask tasknew = new TRECScenarioBRunnable(index,
						"src/main/java/io/anserini/rts/public/TREC2015Profiles/MB" + (new String().valueOf(i))
								+ ".json",
						mailList, startTimestamp);
				LOG.info(tomorrow.getTimeInMillis() + " " + now.getTimeInMillis());
				timer.scheduleAtFixedRate(tasknew, (long) (tomorrow.getTimeInMillis() - now.getTimeInMillis()),
						dailyInterval);
				threadList.add(tasknew);
				timerList.add(timer);
			}
		}
		LOG.info("TRECQueryListernerThread started");

		// for (Thread t : threadList)
		// t.join();

	}

}
