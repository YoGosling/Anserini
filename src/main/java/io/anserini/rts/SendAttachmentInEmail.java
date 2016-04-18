package io.anserini.rts;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.DirectoryNotEmptyException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Properties;

import javax.activation.DataHandler;
import javax.activation.DataSource;
import javax.activation.FileDataSource;
import javax.mail.BodyPart;
import javax.mail.Message;
import javax.mail.MessagingException;
import javax.mail.Multipart;
import javax.mail.PasswordAuthentication;
import javax.mail.Session;
import javax.mail.Transport;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeBodyPart;
import javax.mail.internet.MimeMessage;
import javax.mail.internet.MimeMultipart;

public class SendAttachmentInEmail {

	public static void sendDigestBlockquote(String toAddress, ArrayList<String> tweetList,
			ArrayList<String> userNameList, ArrayList<String> userScreenNameList,
			ArrayList<String> userImageProfileURLList, ArrayList<String> textList, ArrayList<String> epochList,
			String startTimestamp, String other) throws IOException {
		// Recipient's email ID needs to be mentioned.
		String to = toAddress;

		// Sender's email ID needs to be mentioned
		String from = "yogosling@gmail.com";// change accordingly
		final String username = "yogosling";// change accordingly
		final String password = "anserini";// change accordingly

		// Assuming you are sending email through relay.jangosmtp.net
		String host = "smtp.gmail.com";

		Properties props = new Properties();
		props.put("mail.smtp.auth", "true");
		props.put("mail.smtp.starttls.enable", "true");
		props.put("mail.smtp.host", host);
		props.put("mail.smtp.port", "587");

		// Get the Session object.
		Session session = Session.getInstance(props, new javax.mail.Authenticator() {
			protected PasswordAuthentication getPasswordAuthentication() {
				return new PasswordAuthentication(username, password);
			}
		});
		try {
			// Create a default MimeMessage object.
			Message message = new MimeMessage(session);

			// Set From: header field of the header.
			message.setFrom(new InternetAddress(from));

			// Set To: header field of the header.
			if (!to.equals(from))
				message.setRecipients(Message.RecipientType.TO, InternetAddress.parse(to));

			message.setRecipients(Message.RecipientType.BCC, InternetAddress.parse("yogosling@gmail.com"));

			// Set Subject: header field
			message.setSubject(other + "@YoGosling, baseline system testing " + startTimestamp);

			// This mail has 2 part, the BODY and the embedded image
			MimeMultipart multipart = new MimeMultipart("related");

			// first part (the html)
			MimeBodyPart messageBodyPart = new MimeBodyPart();

			String topicDescPath = "src/main/java/io/anserini/rts/public/TREC2015TopicDescHTML/";
			String topicDesc = "";
			BufferedReader in = new BufferedReader(new FileReader(topicDescPath + other.substring(11, 16) + ".html"));
			String thisLine = in.readLine();
			while (thisLine != null) {
				topicDesc = topicDesc + thisLine + "\n";
				thisLine = in.readLine();
			}

			String singleTweetTemplatePath = "src/main/java/io/anserini/rts/public/single_tweet_plus_following_border.html";
			String singleTweetTemplate = "";
			in = new BufferedReader(new FileReader(singleTweetTemplatePath));
			thisLine = in.readLine();
			while (thisLine != null) {
				singleTweetTemplate = singleTweetTemplate + thisLine + "\n";
				thisLine = in.readLine();
			}
			System.out.println(singleTweetTemplate.length());
			String input = "src/main/java/io/anserini/rts/public/print_quotable.html";

			// add it

			String content = "";
			try {
				in = new BufferedReader(new FileReader(input));
				thisLine = in.readLine();
				while (thisLine != null) {
					if (thisLine.length() > 10 && thisLine.substring(0, 10).equals("<!-- stssh")) {
						content = content + topicDesc;
						for (int i = 0; i < tweetList.size(); i++) {
							String thisTweet = "";
							// System.out.println(singleTweetTemplate.length());
							thisTweet = singleTweetTemplate
									.replace("<!-- user profile image url -->", userImageProfileURLList.get(i))
									.replace("<!-- full name here -->", userNameList.get(i))
									.replace("<!-- screen name here -->", " @" + userScreenNameList.get(i))
									.replace("<!-- tweet text here -->",
											textList.get(i) + "<br>&mdash; " + epochList.get(i))
									.replace("<!-- raw address here -->", "https://twitter.com/"
											+ userScreenNameList.get(i) + "/status/" + tweetList.get(i));
							// System.out.println(thisTweet.length());

							content = content + thisTweet + "\n";
						
						}
					} else
						content = content + "\n" + thisLine;
					thisLine = in.readLine();
				}

				// out.close();
			} catch (IOException e) {
				System.err.println("Error: " + e.getMessage());
			}
			// System.out.println(content);
			messageBodyPart.setContent(content, "text/html");
			messageBodyPart.setHeader("Content-Type", "text/html;charset=\"utf-8\"");
			messageBodyPart.setHeader("Content-Transfer-Encoding", "quoted-printable");

			multipart.addBodyPart(messageBodyPart);

			// Send the complete message parts
			message.setContent(multipart);

			// Send message
			Transport.send(message);

			System.out.println("Sent message successfully....");

			// default 7bit encoding
			System.out.println("Its transfer encoding in header is " + ((MimeBodyPart) messageBodyPart).getEncoding());

		} catch (

		MessagingException e)

		{
			throw new RuntimeException(e);
		}

	}

	public static void main(String[] args) throws IOException {
	}
}