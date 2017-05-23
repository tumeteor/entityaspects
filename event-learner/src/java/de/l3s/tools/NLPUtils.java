package de.l3s.tools;

import java.io.StringReader;
import java.util.HashMap;
import java.util.List;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import edu.stanford.nlp.ie.crf.CRFClassifier;
import edu.stanford.nlp.ling.CoreLabel;
import edu.stanford.nlp.ling.Sentence;
import edu.stanford.nlp.objectbank.TokenizerFactory;
import edu.stanford.nlp.parser.lexparser.LexicalizedParser;
import edu.stanford.nlp.process.CoreLabelTokenFactory;
import edu.stanford.nlp.process.PTBTokenizer;
import edu.stanford.nlp.trees.Tree;
import edu.stanford.nlp.trees.tregex.TregexMatcher;
import edu.stanford.nlp.trees.tregex.TregexPattern;

/**
 * Extracting named-entities from text
 * @author tunguyen
 *
 */
public class NLPUtils {
	public static final String PERSON = "<PERSON>";
	public static final String ORGANIZATION = "<ORGANIZATION>";
	public static final String LOCATION = "<LOCATION>";

	String serializedClassifier = "classifiers/english.all.3class.distsim.crf.ser.gz";
	protected static CRFClassifier classifier;
	static LexicalizedParser lp;
	static TokenizerFactory<CoreLabel> tokenizerFactory;
	static TregexPattern p;
	static final String pattern = "<(\\w+)>.*?</\\1>";
	static final Pattern wpattern = Pattern.compile("(?:^|\\s)[a-zA-Z,_]+(?=\\s|$)");
	Pattern r;
	HashMap<String, String> entityTypeMap = new HashMap<String, String>();
	Set<Entry<String, String>> entities;
	public NLPUtils(){
		Properties props = new Properties();
		props.setProperty("inputEncoding", "UTF-8");
		classifier = new CRFClassifier(props);
		classifier.loadClassifierNoExceptions(serializedClassifier, props);
		classifier.flags.setProperties(props);
		lp = LexicalizedParser.loadModel("englishPCFG.ser.gz");
		tokenizerFactory = PTBTokenizer.factory(new CoreLabelTokenFactory(), "");
		p = TregexPattern.compile("@NP");
		r = Pattern.compile(pattern);
	}
	/**
	 * 
	 * @param sentence
	 * @return
	 */
	public String extractEntitiesfromSentence(String sentence){
		entityTypeMap.clear();
		List<CoreLabel> term = tokenizerFactory.getTokenizer(
				new StringReader(sentence)).tokenize();
		TregexMatcher matcher = p.matcher(lp.apply(term));
		String nounPhrase;
		Tree match;
		String annotatedTerm;
		Matcher m;
		String xmlElement;
		String pTerm;
		String oTerm;
		String lTerm;
		while (matcher.findNextMatchingNode()) {
			match = matcher.getMatch();
			nounPhrase = Sentence.listToString(match.yield());	
			annotatedTerm = classifier.classifyWithInlineXML(nounPhrase);
			m = r.matcher(annotatedTerm);
			while (m.find()) {
				xmlElement = m.group();
				if (xmlElement.contains(PERSON)){
					pTerm = xmlElement.substring(xmlElement.indexOf(PERSON)+PERSON.length(),
							xmlElement.indexOf("</PERSON>"));
					if(!entityTypeMap.containsKey(pTerm))
						entityTypeMap.put(pTerm, "per:");
				}
				if (xmlElement.contains(ORGANIZATION )){
					oTerm = xmlElement.substring(xmlElement.indexOf(ORGANIZATION)+ORGANIZATION.length(),
							xmlElement.indexOf("</ORGANIZATION>"));
					if(!entityTypeMap.containsKey(oTerm))
						entityTypeMap.put(oTerm, "org:");	
				}
				if (xmlElement.contains(LOCATION)){
					lTerm = xmlElement.substring(xmlElement.indexOf(LOCATION)+LOCATION.length(),
							xmlElement.indexOf("</LOCATION>"));
					if(!entityTypeMap.containsKey(lTerm))
						entityTypeMap.put(lTerm, "loc:");	
				}
			}
		}
		entities = entityTypeMap.entrySet();
		StringBuilder entityListBuilder = new StringBuilder();
		for(Entry<String, String> entry: entities){
			entityListBuilder.append(entry.getValue()).append(entry.getKey()).append(", ");
		}
		System.out.println(entityListBuilder.toString());
		return entityListBuilder.toString();

	}

	public String ignoreNonCharWords(String text) {
		StringBuilder sb = new StringBuilder();
		String[] words = text.split(" ");
		for (String word : words) {
			word = word.replaceAll("_", " ");
			System.out.println("word: " + word);
			if (word.contains("%") || word.trim().equals("")) continue;
			Matcher matcher = wpattern.matcher(word);
			if (matcher.find()) {
				sb.append(word).append(" ");
			}	
		}
		return sb.toString();
	}

	public static void main (String[] args){
		NLPUtils NEExtractor = new NLPUtils();
		//	String text = NEExtractor.ignoreNonCharWords("Kabul, Mazar-i-Sharif");

		NEExtractor.extractEntitiesfromSentence("Brazil won the Worldcup");

	}

}
