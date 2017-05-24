package l3s.de.event.features;

import org.apache.lucene.document.Document;
import org.apache.lucene.index.*;
import org.apache.lucene.search.*;
import org.apache.lucene.search.spans.SpanMultiTermQueryWrapper;
import org.apache.lucene.search.spans.SpanNearQuery;
import org.apache.lucene.search.spans.SpanQuery;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;

import java.io.File;
import java.io.IOException;
import java.util.*;

public class KLDivergence {
    static final List<String> important_terms = Arrays.asList("election",
            "world series",
            "world cup",
            "commonwealth games", "nba", "ncaa", "march madness", "nfl",
            "easter", "emmy", " golden globe",
            "hurricane", "kentucky derby", "mtv",
            "preakness",
            "american idol", "next top model", "april fool", "waterloo",
            "cannes", "cinco", "csi", "earth", "mother's day", "father's day", "graduation", "lennon",
            "mlb", "nit", "ozzfest", "pentagon", "pga", "war", "may day", "teacher appreciation", "atlanta braves", "olympics"
    );
    public IndexReader reader;
    public IndexSearcher searcher;
    String indexDir = "/data/tunguyen/apache-solr-3.1.0/solr/data/index";
    double sumOfTfInColl = 0.0d;
    double sumOfPtfInColl = 0.0d;
    HashMap<String, Integer> termFreqInColl = new HashMap<String, Integer>();
    HashMap<String, Integer> ptFreqInColl = new HashMap<String, Integer>();
    private double lambda = 0.1d;

    public static void main(String[] args) {
        KLDivergence kl = new KLDivergence();
        String[] queries = {"obama", "barack obama", "cinco de mayo", "world cup", "chrismas"};
        try {
            kl.init();
            for (String query : queries) {
                kl.computeTemporalKL_PT(query);
            }
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    public void init() {
        Directory dir;
        try {
            dir = FSDirectory.open(new File(indexDir));
            reader = IndexReader.open(dir);
            searcher = new IndexSearcher(reader);
            TermEnum terms = reader.terms();
            while (terms.next()) {
                Term lucene_term = terms.term();
                if (lucene_term.field().equals("text")) {
                    String term = terms.term().text();
                    TermDocs termDocs = reader.termDocs(new Term("text", term));
                    int tfInCol = 0;
                    while (termDocs.next()) {
                        tfInCol += termDocs.freq();
                    }
                    sumOfTfInColl += tfInCol;
                    termFreqInColl.put(term, tfInCol);
                } else if (lucene_term.field().equals("pubTime")) {
                    String pubTime = lucene_term.text();
                    TermDocs pubTimeDocs = reader.termDocs(new Term("pubTime", pubTime));
                    int ptfInCol = 0;
                    while (pubTimeDocs.next()) {
                        ptfInCol += pubTimeDocs.freq();
                    }
                    sumOfPtfInColl += ptfInCol;
                    ptFreqInColl.put(pubTime, ptfInCol);
                }

            }
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    /**
     * Method for computing clarity score for a query (no time, only terms in the field "text" are consider).
     * clarity = sum_(w in V) P(w|q)*log2 P(w|q)/P(w|C)
     * P(w|q) = sum(d in topKDoc) P(w|d)*P(d|q)
     * P(d|q) = P(q|d) = II_(q' in q) P(q'|d)
     * P(q'|d) = II_(q' in d) (1-lambda)*P'(q'|d) * II_(q' not in d) lambda*P(q'|C)
     * P(w|d) = (1-lambda)*P'(w|d) + lambda*P(w|C)
     *
     * @param topDocsOfQ
     * @return
     * @throws IOException
     */
    public double computeClarityScore(String query_term, String[] queries) throws IOException {
        double scoreClarity = 0.0;
        Query query;
        if (query_term.contains(" ") || query_term.contains("_")) {
            String[] terms = query_term.split("[\\s_]+");
            BooleanQuery booleanQuery = new BooleanQuery();
            for (String t : terms) {
                booleanQuery.add(new TermQuery(
                                new Term("text", t)),
                        BooleanClause.Occur.MUST);
            }
            query = booleanQuery;
        } else {
            query = new TermQuery(new Term("text", query_term));
        }
        TopScoreDocCollector collector = TopScoreDocCollector.create(1000, true);
        searcher.search(query, collector);
        ScoreDoc[] hits = collector.topDocs().scoreDocs;
        ArrayList<Document> retDocuments = new ArrayList<Document>();
        ArrayList<Integer> textDocTS = new ArrayList<Integer>();
        for (ScoreDoc hit : hits) {
            retDocuments.add(searcher.doc(hit.doc));
            textDocTS.add(hit.doc);
        }
        Iterator<String> termItr = termFreqInColl.keySet().iterator();
        while (termItr.hasNext()) {
            String term = termItr.next();
            double tfInColl = termFreqInColl.get(term);
            double pTermInColl = tfInColl / sumOfTfInColl;

            double pTermInQ = 0.0;
            for (int d = 0; d < retDocuments.size(); d++) {
                TermFreqVector termFreqInDoc = reader.getTermFreqVector(textDocTS.get(d), "text");

                double tf = 0.0d;
                double sumOfTf = 0.0d;
                int[] termFreqs = termFreqInDoc.getTermFrequencies();
                for (int tf_ : termFreqs) {
                    sumOfTf += tf_;
                }
                List<String> termsInDoc = Arrays.asList(termFreqInDoc.getTerms());
                if (termsInDoc.contains(term)) {
                    tf = termFreqs[termsInDoc.indexOf(term)];
                }
                double pTermInDoc = (1 - lambda) * (tf / sumOfTf) + lambda * (pTermInColl);

                double pDocGivenQ = 1.0;
                for (int q = 0; q < queries.length; q++) {
                    String qTerm = queries[q];
                    if (termsInDoc.contains(qTerm)) {
                        double qTf = termFreqs[termsInDoc.indexOf(qTerm)];
                        pDocGivenQ = pDocGivenQ * ((1 - lambda) * qTf / sumOfTf);
                    } else {
                        double qTfInColl = 0.0;
                        if (termFreqInColl.containsKey(qTerm)) {
                            qTfInColl = termFreqInColl.get(qTerm);
                        } else {
                            qTfInColl = 1;
                        }
                        pDocGivenQ = pDocGivenQ * (lambda * qTfInColl / sumOfTfInColl);
                    }
                }

                pTermInQ += pTermInDoc * pDocGivenQ;
            }

            scoreClarity += pTermInQ * Math.log10(pTermInQ / pTermInColl);
        }
        System.out.println("Clarity Score: " + scoreClarity);
        return scoreClarity;
    }

    public String cleanQuery(String term) {
        term = term.replace("loved", "");
        term = term.replace("craigs list", "craigslist");
        term = term.replace(".com", "");
        term = term.replace("spiderman", "spider-man");
        term = term.replace("warp tour", "warped tour");
        term = term.replace("xmen", "x-men");
        term = term.replace("mothersday", "mother's day");
        term = term.replace("happy", "");
        term = term.replace("kingdomhearts", "kingdom hearts");
        term = term.replace("www.", "");
        term = term.replace("www", "");
        term = term.replace("poems", "");
        term = term.replace("fathers", "father's");
        // remove year for explicit queries
        term = term.replaceAll("(19|20)\\d{2}", "");

        for (String impt : important_terms) {
            if (term.contains(impt))
                return impt;
        }
        return term;
    }

    /**
     * Method for determining temporal KL-divergence of a query from the distribution of collection publication dates
     * - A temporal KL score is computed using the distribution of publication dates of top-k retrieved documents
     * - The higher a score, the more time-sensitive
     * - Temporal KL is defined in Jones and Diaz TOIS2006 as follows:
     * - TKL(q||C) = SUM_pt P(pt|q) * log P(pt|q)/P(pt|C)
     * - P(pt|q) = lambda*P'(pt|q) + (1.0-lambda)*P'(pt|C)
     * - P'(pt|q) = SUM_Dq P(pt|d) * P(q|d)/(SUM_(d" in Dq) P(q|d"))
     * - P'(pt|C) = 1/|C| * SUM_(d in C) P(pt|d)
     * - P(pt|d) = 1 iff pt = pubDate(d)
     *
     * @throws IOException
     */
    public double computeTemporalKL_PT(String query) throws IOException {
        double score_KL = 0.0;
        query = cleanQuery(query);
        String[] terms = query.split("[\\s_]+");
        SpanQuery[] clauses = new SpanQuery[terms.length];

        for (int i = 0; i < terms.length; i++) {
            clauses[i] = new SpanMultiTermQueryWrapper(new FuzzyQuery(new Term(
                    "text", terms[i])));
        }

        SpanNearQuery snquery = new SpanNearQuery(clauses, 0, true);

        TopScoreDocCollector collector = TopScoreDocCollector.create(1000, true);
        searcher.search(snquery, collector);
        ScoreDoc[] hits = collector.topDocs().scoreDocs;
        ArrayList<Document> retDocuments = new ArrayList<Document>();
        ArrayList<Float> textDocTS = new ArrayList<Float>();
        Float sumOfRetScores = 0.0f;
        for (ScoreDoc hit : hits) {
            retDocuments.add(searcher.doc(hit.doc));
            textDocTS.add(hit.score);
            sumOfRetScores += hit.score;
        }

        if ((retDocuments != null) && (retDocuments.size() > 0)) {
            int numTopDoc = retDocuments.size();

            // First round, compute KL divergence and background smoothing
            Iterator<String> pubTimeItr = ptFreqInColl.keySet().iterator();
            while (pubTimeItr.hasNext()) {
                String pubTime = pubTimeItr.next();
                double pPubTimeInColl = 1.0 / sumOfPtfInColl * (double) ptFreqInColl.get(pubTime);
                double pPubTimeInQ = 0.0;
                for (int d = 0; d < numTopDoc; d++) {
                    Document currDoc = retDocuments.get(d);
                    double pPubTimeInDoc = 0.0;
                    if (currDoc.getFieldable("pubTime") != null && currDoc.getFieldable("pubTime").stringValue().equals(pubTime)) {
                        pPubTimeInDoc = 1.0;
                    }
                    double retScore = textDocTS.get(d) / sumOfRetScores;
                    pPubTimeInQ += pPubTimeInDoc * retScore;
                }
                double pPubTimeInQ_smooth = (1.0 - lambda) * pPubTimeInQ + lambda * pPubTimeInColl;
                score_KL += pPubTimeInQ_smooth * Math.log10(pPubTimeInQ_smooth / pPubTimeInColl);
            }
        }
        System.out.println("KL Score: " + score_KL);
        return score_KL;
    }

}