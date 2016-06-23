YoGosling
========

###Build with Maven

```
mvn clean package appassembler:assemble
```

YoGosling is a branch from Anserini[https://github.com/lintool/Anserini] project. Like Anserini, To run YoGosling, you must save your Twitter API OAuth credentials in a file named `twitter4j.properties` in your current working YoGosling root directory. See [this page](http://twitter4j.org/en/configuration.html) for more information about Twitter4j configurations. The file should contain the following (replace the ********** instances with your information):

	oauth.consumerKey=**********
	oauth.consumerSecret=**********
	oauth.accessToken=**********
	oauth.accessTokenSecret=**********

###Index and search 

```
sh target/appassembler/bin/TRECSearcher -groupid uwar -index <index_name>  -host localhost -port 3333 
```

`-groupid -host -port` options are the corresponding values that participants self-configured in a local broker server. Details for configuring a local broker server can be found [here](https://github.com/trecrts/trecrts-eval)


Following is where each [evaluation broker REST(ful) API](https://github.com/trecrts/trecrts-eval/tree/master/trecrts-server) call is implemented. 

POST /register/system 

[https://github.com/YoGosling/Anserini/blob/master/src/main/java/io/anserini/rts/Registrar.java#L26](https://github.com/YoGosling/Anserini/blob/master/src/main/java/io/anserini/rts/Registrar.java#L26)

GET /topics/:clientid

[https://github.com/YoGosling/Anserini/blob/master/src/main/java/io/anserini/rts/TopicPoller.java#L33](https://github.com/YoGosling/Anserini/blob/master/src/main/java/io/anserini/rts/TopicPoller.java#L33)

POST /tweet/:topid/:tweetid/:clientid

[https://github.com/YoGosling/Anserini/blob/master/src/main/java/io/anserini/rts/TRECScenarioRunnable.java#L168](https://github.com/YoGosling/Anserini/blob/master/src/main/java/io/anserini/rts/TRECScenarioRunnable.java#L168)


###Algorithm

YoGosling is a modified and extended version of the best performing automatic system in TREC 2015. For algorithm details, please refer to the paper, [University of Waterloo at TREC 2015 Microblog Track](http://trec.nist.gov/pubs/trec24/papers/UWaterlooMDS-MB.pdf)

####Relevance Scoring Example: Star Wars 

Document #298: { coins } ll 2016 Niue $2 1 oz. Proof Silver Star Wars Classics Series - Han Solo | GEM Proof (Original Mint ... [link](https://t.co/6pQTdwW9Iw) 

Interest Profile: Star Wars

	titleQuery: text:star text:wars 

	titleCoordSimilarity = 2/2 = 1.0 

	titleExpansionQuery: +(text:star^3.0 text:wars^3.0) #epoch:[1464847032 TO 1464847092]
	
	titleExpansionSimilarity = 6.0 (as follows)
	
	finalSimilarityScore = 1.0 * 6.0 = 6.0

YoGosling log snippet

```
2016-06-02 13:58:12,592 INFO  [Timer-2] rts.TRECScenarioRunnable (TRECScenarioRunnable.java:305) - 6.0 = sum of:
  3.0 = weight(text:star^3.0 in 298) [TitleExpansionSimilarity], result of:
    3.0 = score(doc=298,freq=1.0), product of:
      3.0 = queryWeight, product of:
        3.0 = boost
        1.0 = idf(docFreq=1, maxDocs=445)
        1.0 = queryNorm
      1.0 = fieldWeight in 298, product of:
        1.0 = tf(freq=1.0), with freq of:
          1.0 = termFreq=1.0
        1.0 = idf(docFreq=1, maxDocs=445)
        1.0 = fieldNorm(doc=298)
  3.0 = weight(text:wars^3.0 in 298) [TitleExpansionSimilarity], result of:
    3.0 = score(doc=298,freq=1.0), product of:
      3.0 = queryWeight, product of:
        3.0 = boost
        1.0 = idf(docFreq=1, maxDocs=445)
        1.0 = queryNorm
      1.0 = fieldWeight in 298, product of:
        1.0 = tf(freq=1.0), with freq of:
          1.0 = termFreq=1.0
        1.0 = idf(docFreq=1, maxDocs=445)
        1.0 = fieldNorm(doc=298)

2016-06-02 13:58:12,592 INFO  [Timer-2] rts.TRECScenarioRunnable (TRECScenarioRunnable.java:306) - Multiplied by 1.0 Final score 6.0
2016-06-02 13:58:12,592 INFO  [Timer-2] rts.TRECScenarioRunnable (TRECScenarioRunnable.java:308) - Raw text{ coins } ll 2016 Niue $2 1 oz. Proof Silver Star Wars Classics Series - Han Solo | GEM Proof (Original Mint ... https://t.co/6pQTdwW9Iw 2

```





Anserini
========

### Twitter (Near) Real-Time Search

To get access to the Twitter public stream, you need a developer account to obtain OAuth credentials. After creating an account on the Twitter developer site, you can obtain these credentials by [creating an "application"](https://dev.twitter.com/apps/new). After you've created an application, create an access token by clicking on the button "Create my access token".

To to run the Twitter (near) real-time search demo, you must save your Twitter API OAuth credentials in a file named `twitter4j.properties` in your current working directory. See [this page](http://twitter4j.org/en/configuration.html) for more information about Twitter4j configurations. The file should contain the following (replace the `**********` instances with your information):

```
oauth.consumerKey=**********
oauth.consumerSecret=**********
oauth.accessToken=**********
oauth.accessTokenSecret=**********
```

Once you've done that, fire up the demo with:

```
sh target/appassembler/bin/TweetSearcher -index twitter-index
```

The demo starts up an HTTP server on port `8080`, but this can be changed with the `-port` option. Query via a web browser at `http://localhost:8080/search?query=query`. Try `birthday`, as there are always birthdays being celebrated. 

User could change the maximum number of hits returned at 'http://localhost:8080/search?query=birthday&top=15'. The default number of hits is 20. 
