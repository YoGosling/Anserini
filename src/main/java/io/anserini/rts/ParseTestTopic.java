package io.anserini.rts;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;

import com.google.gson.JsonObject;

public class ParseTestTopic {
	public class InterestProfile {
		String num;
		String title;
		String Description;
		String Narrative;

		public InterestProfile(String num, String title, String Desc, String Narr) {
			this.num = num;
			this.title = title;
			this.Description = Desc;
			this.Narrative = Narr;
		}
	}

	public static void main(String[] args) throws FileNotFoundException, IOException {
		// TODO Auto-generated method stub
		ArrayList<InterestProfile> mb = new ArrayList<InterestProfile>();
		String JSONObjectString = "";
		try (BufferedReader br = new BufferedReader(
				new FileReader("src/main/java/io/anserini/rts/public/TREC2015-MB-testtopics.txt"))) {
			String line = br.readLine();
			while (line != null) {

				if (line.length() >= 5 && line.substring(0, 5).equals("<top>")) {
					JsonObject obj = new JsonObject();
					line = br.readLine();
					String num = line.substring(14);
					line = br.readLine();
					line = br.readLine();
					String title = br.readLine();
					line = br.readLine();
					line = br.readLine();
					String description = "";
					line = br.readLine();
					while (!line.substring(0, Math.min(line.length(),6)).equals("<narr>")) {
						description = description + line;
						line = br.readLine();

					}
					String narr = "";
					line = br.readLine();
					while (line.length() >= 6 && !line.substring(0, Math.min(line.length(),6)).equals("</top>")) {
						narr = narr + line;
						line = br.readLine();

					}
					mb.add(new ParseTestTopic().new InterestProfile(num, title, description, narr));

					BufferedReader in = new BufferedReader(
							new FileReader("src/main/java/io/anserini/rts/public/topic_template.html"));
					String thisLine = in.readLine();
					String singleTweetTemplate = "";
					while (thisLine != null) {
						singleTweetTemplate = singleTweetTemplate + thisLine + "\n";
						thisLine = in.readLine();
					}
					singleTweetTemplate = singleTweetTemplate.replace("<!-- num here -->", num)
							.replace("<!-- title here -->", title).replace("<!-- Desc here -->", description)
							.replace("<!-- Narr here -->", narr);
					try (FileWriter file = new FileWriter(
							"src/main/java/io/anserini/rts/public/TREC2015TopicDescHTML/" + num + ".html")) {
						file.write(singleTweetTemplate);
						System.out.println("Successfully Copied Json Object to File...");
						System.out.println("\nJson Object: " + obj);
						file.close();
					}

				}
				line = br.readLine();

			}
			for (InterestProfile i : mb) {
				System.out.println(i.num + " " + i.title + " " + i.Description + " " + i.Narrative);
			}

		}

	}

}
