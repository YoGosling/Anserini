package io.anserini.rts;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;


public class ParseInterestProfile {
	private static final JsonParser JSON_PARSER = new JsonParser();

	public static void main(String[] args) throws FileNotFoundException, IOException {
		// TODO Auto-generated method stub

		String JSONObjectString = "";
		try (BufferedReader br = new BufferedReader(
				new FileReader("src/main/java/io/anserini/rts/public/profiles_with9_empty_expansion_fields"))) {
			String line = br.readLine();

			while (line != null) {
				JSONObjectString = JSONObjectString + line;
				line = br.readLine();
			}
		}

		JsonObject interestProfileObject = (JsonObject) JSON_PARSER.parse(JSONObjectString);
		for (JsonElement o : interestProfileObject.getAsJsonArray("profiles")) {
			JsonObject obj = new JsonObject();
			obj.add("index", ((JsonObject) o).get("num"));
			obj.add("query", ((JsonObject) o).get("title"));
			obj.add("expansion", ((JsonObject) o).get("expansion"));

			// try-with-resources statement based on post comment below :)
			try (FileWriter file = new FileWriter("src/main/java/io/anserini/rts/public/TREC2015Profiles/"+((JsonObject) o).get("num").toString().substring(1, 6)+".json")) {
				file.write(obj.toString());
				System.out.println("Successfully Copied Json Object to File...");
				System.out.println("\nJson Object: " + obj);
			}

			System.out.println(((JsonObject) o).get("num"));
		}
	}

}
