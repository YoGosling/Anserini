package io.anserini.rts;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
public class TRECTopic {

	@JsonProperty("topid")
	public String topid;
	@JsonProperty("title")
	public String title;
	@JsonProperty("body")
	public String body;

	@JsonCreator
	public TRECTopic(@JsonProperty("topid") String topicID, @JsonProperty("title") String title,
			@JsonProperty("body") String body) {
		super();
		this.topid = topicID;
		this.title = title;
		this.body = body;
	}
}
