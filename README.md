YoGosling
========

Build using Maven:

```
mvn clean package appassembler:assemble
```

Build index 

```
nohup sh target/appassembler/bin/TRECIndexer -index <index_name>    > nohup.out &

```

Search with either Scenario A or B

```
nohup sh target/appassembler/bin/TRECScenarioSearcher -index <index_name> -mailList <email_address, comma separated> -topicSequence <topic_id, 3 digits, colon separated> -scenario <A/B> > nohup.out &

e.g. nohup sh target/appassembler/bin/TRECScenarioSearcher -index 160418 -mailList yogosling@gmail.com,helloanserini@gmail.com -topicSequence 313:242:265:292:390:274:262:415 -scenario A > nohup_4452.out &

```

Index diagnostic page, via SOCKS proxy (ssh to remote server and build a tunnel)

```
http://www.startupcto.com/server-tech/macosx/setting-up-a-socks-proxy-in-mac-osx
```

For Mac user, Firefox is required

```
sh target/appassembler/bin/TweetSearcher -index 20160419 -port 8080
```
After the above configuration, you are able to visit the diagnostic page by address localhost:8080



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
