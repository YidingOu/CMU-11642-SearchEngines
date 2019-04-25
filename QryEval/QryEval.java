
/*
 *  Copyright (c) 2019, Carnegie Mellon University.  All Rights Reserved.
 *  Version 3.3.3.
 */
import java.io.*;
import java.util.*;

import org.apache.lucene.analysis.Analyzer.TokenStreamComponents;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.*;
import org.apache.lucene.search.*;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.Version;

/**
 * This software illustrates the architecture for the portion of a search engine
 * that evaluates queries. It is a guide for class homework assignments, so it
 * emphasizes simplicity over efficiency. It implements an unranked Boolean
 * retrieval model, however it is easily extended to other retrieval models. For
 * more information, see the ReadMe.txt file.
 */
public class QryEval {

  // --------------- Constants and variables ---------------------

  private static final String USAGE = "Usage:  java QryEval paramFile\n\n";

  private static final String[] TEXT_FIELDS = { "body", "title", "url", "inlink" };

  // --------------- Methods ---------------------------------------

  /**
   * @param args The only argument is the parameter file name.
   * @throws Exception Error accessing the Lucene index.
   */
  public static void main(String[] args) throws Exception {

    // This is a timer that you may find useful. It is used here to
    // time how long the entire program takes, but you can move it
    // around to time specific parts of your code.

    Timer timer = new Timer();
    timer.start();

    // Check that a parameter file is included, and that the required
    // parameters are present. Just store the parameters. They get
    // processed later during initialization of different system
    // components.

    if (args.length < 1) {
      throw new IllegalArgumentException(USAGE);
    }

    Map<String, String> parameters = readParameterFile(args[0]);

    // Open the index and initialize the retrieval model.

    Idx.open(parameters.get("indexPath"));
    RetrievalModel model = initializeRetrievalModel(parameters);

    // Perform experiments.
    if (parameters.containsKey("diversity") && parameters.get("diversity").equals("true")) {
      Diversification diver = new Diversification(parameters);
      diver.process(model);
    } else if (!(model instanceof RetrievalModelLetor)) {
      processQueryFile(parameters.get("queryFilePath"), parameters.get("trecEvalOutputLength"),
          parameters.get("trecEvalOutputPath"), model, parameters);
    }

    // Clean up.

    timer.stop();
    System.out.println("Time:  " + timer);
  }

  /**
   * Allocate the retrieval model and initialize it using parameters from the
   * parameter file.
   * 
   * @return The initialized retrieval model
   * @throws IOException Error accessing the Lucene index.
   */
  private static RetrievalModel initializeRetrievalModel(Map<String, String> parameters) throws Exception, IOException {

    RetrievalModel model = null;
    if (!parameters.containsKey("retrievalAlgorithm")) {
      return model;
    }
    String modelString = parameters.get("retrievalAlgorithm").toLowerCase();
    // System.out.println(modelString);
    if (modelString.equals("unrankedboolean")) {
      model = new RetrievalModelUnrankedBoolean();
    } else if (modelString.equals("rankedboolean")) {
      model = new RetrievalModelRankedBoolean();
    } else if (modelString.equals("bm25")) {
      double k_1 = Double.valueOf(parameters.get("BM25:k_1"));
      double b = Double.valueOf(parameters.get("BM25:b"));
      double k_3 = Double.valueOf(parameters.get("BM25:k_3"));
      model = new RetrievalModelBM25(k_1, b, k_3);
    } else if (modelString.equals("indri")) {
      double mu = Double.valueOf(parameters.get("Indri:mu"));
      double lambda = Double.valueOf(parameters.get("Indri:lambda"));
      model = new RetrievalModelIndri(mu, lambda);
    } else if (modelString.equals("letor")) {
      model = new RetrievalModelLetor(parameters);
      RetrievalModelLetor letor = (RetrievalModelLetor) model;
      letor.process();
    } else {
      throw new IllegalArgumentException("Unknown retrieval model " + parameters.get("retrievalAlgorithm"));
    }

    return model;
  }

  /**
   * Print a message indicating the amount of memory used. The caller can indicate
   * whether garbage collection should be performed, which slows the program but
   * reduces memory usage.
   * 
   * @param gc If true, run the garbage collector before reporting.
   */
  public static void printMemoryUsage(boolean gc) {

    Runtime runtime = Runtime.getRuntime();

    if (gc)
      runtime.gc();

    System.out.println("Memory used:  " + ((runtime.totalMemory() - runtime.freeMemory()) / (1024L * 1024L)) + " MB");
  }

  /**
   * Process one query.
   * 
   * @param qString A string that contains a query.
   * @param model   The retrieval model determines how matching and scoring is
   *                done.
   * @return Search results
   * @throws IOException Error accessing the index
   */
  static ScoreList processQuery(String qString, RetrievalModel model) throws IOException {

    String defaultOp = model.defaultQrySopName();
    // System.out.println("before " + qString);
    qString = defaultOp + "(" + qString + ")";
    // System.out.println("after " + qString);
    Qry q = QryParser.getQuery(qString);

    // Show the query that is evaluated

    System.out.println("    --> " + q);

    if (q != null) {
      // System.out.println(q.getClass().getName());
      ScoreList r = new ScoreList();

      if (q.args.size() > 0) { // Ignore empty queries

        q.initialize(model);

        while (q.docIteratorHasMatch(model)) {
          int docid = q.docIteratorGetMatch();
          // System.out.println(Idx.getInternalDocid("GX000-48-5866977") );
          // System.out.println(Idx.getExternalDocid(0));
          // if (Idx.getExternalDocid(docid).equals("GX022-93-1111575")) {
          // System.out.println(docid);
          // }

          // 104413 533875
          double score = ((QrySop) q).getScore(model);
          r.add(docid, score);
          q.docIteratorAdvancePast(docid);

        }

      }

      return r;
    } else
      return null;
  }

  static String expandQuery(ScoreList list, Map<String, String> parameters) throws IOException {
    int fbDocs = Integer.parseInt(parameters.get("fbDocs"));
    int fbTerms = Integer.parseInt(parameters.get("fbTerms"));
    double fbMu = Double.parseDouble(parameters.get("fbMu"));
    Map<String, Double> scoreMap = new HashMap<>();
    Map<Integer, TermVector> cache = new HashMap<>();
    Map<String, Double> cache2 = new HashMap<>();

    for (int i = 0; i < fbDocs; i++) {
      int docid = list.getDocid(i);
      TermVector tv = new TermVector(docid, "body");
      cache.put(docid, tv);
      for (int terms = 1; terms < tv.stemsLength(); terms++) {
        String term = tv.stemString(terms);
        // System.out.println(term);
        if (term.indexOf('.') >= 0 || term.indexOf(',') >= 0) {
          continue;
        }
        double pMle = 0.0;
        if (cache2.containsKey(term)) {
          pMle = cache2.get(term);
        } else {
          pMle = (double) Idx.getTotalTermFreq("body", term) / (double) Idx.getSumOfFieldLengths("body");
          cache2.put(term, pMle);
        }
        double score = calScore(pMle, tv.stemFreq(terms), fbMu, list.getDocidScore(i), docid);

        Double tmp = scoreMap.get(term);
        if (tmp == null) {
          scoreMap.put(term, score);
        } else {
          scoreMap.put(term, tmp + score);
        }
      }
    }

    for (Map.Entry<String, Double> entry : scoreMap.entrySet()) {
      String term = entry.getKey();
      for (int i = 0; i < fbDocs; i++) {
        int docid = list.getDocid(i);
        TermVector tv = cache.get(docid);
        int index = tv.indexOfStem(term);
        double pMle = cache2.get(term);
        double score = 0.0;
        if (index == -1) {
          score = calScore(pMle, 0.0, fbMu, list.getDocidScore(i), docid);
        } else {
          continue;
        }
        Double tmpScore = scoreMap.get(term);
        if (tmpScore == null) {
          scoreMap.put(term, score);
        } else {
          scoreMap.put(term, tmpScore + score);
        }
      }
    }

    StringBuilder sb = new StringBuilder();
    sb.append("#wand(");
    PriorityQueue<Map.Entry<String, Double>> minHeap = new PriorityQueue<>(new Comparator<Map.Entry<String, Double>>() {
      @Override
      public int compare(Map.Entry<String, Double> e1, Map.Entry<String, Double> e2) {
        if (e1.getValue() == e2.getValue()) {
          return 0;
        }
        return e1.getValue() < e2.getValue() ? -1 : 1;
      }
    });
    for (Map.Entry<String, Double> entry : scoreMap.entrySet()) {
      minHeap.offer(entry);
      if (minHeap.size() > fbTerms) {
        minHeap.poll();
      }
    }

    while (!minHeap.isEmpty()) {
      Map.Entry<String, Double> tmp = minHeap.poll();
      String round = String.format("%.4f", tmp.getValue());
      sb.append(" " + round + " " + tmp.getKey());
    }
    sb.append(")");
    System.out.println(sb.toString());
    return sb.toString();

  }

  static Map<Integer, ScoreList> readInitialRankingFile(String initialPath) {
    Map<Integer, ScoreList> map = new HashMap<>();
    try {
      BufferedReader reader = new BufferedReader(new FileReader(initialPath));
      String line = reader.readLine();
      ScoreList score = new ScoreList();
      int qid = Integer.valueOf(line.split(" ")[0].trim());
      int prev = qid;
      while (line != null) {
        String[] array = line.split(" ");
        qid = Integer.valueOf(array[0].trim());

        if (qid != prev) {
          map.put(prev, score);
          prev = qid;
          score = new ScoreList();
        }
        score.add(Idx.getInternalDocid(array[2].trim()), Double.valueOf(array[4].trim()));
        line = reader.readLine();
      }
      map.put(qid, score);
      reader.close();
    } catch (Exception e) {
      e.printStackTrace();
    }
    return map;
  }

  private static double calScore(double pMle, double tf, double fbMu, double docScore, int docid) throws IOException {

    double td = (tf + (fbMu * pMle)) / (Idx.getFieldLength("body", docid) + fbMu);
    double idf = Math.log(1 / pMle);
    double score = td * idf * docScore;
    return score;
  }

  /**
   * Process the query file.
   * 
   * @param queryFilePath
   * @param model
   * @throws IOException Error accessing the Lucene index.
   */
  static void processQueryFile(String queryFilePath, String trecEvalOutputLength, String trecEvalOutputPath,
      RetrievalModel model, Map<String, String> parameters) throws IOException {

    BufferedReader input = null;

    try {
      String qLine = null;

      File outputFile = new File(trecEvalOutputPath);

      input = new BufferedReader(new FileReader(queryFilePath));

      int outputLength;
      if (parameters.containsKey("diversity:maxResultRankingLength")) {
        outputLength = Integer.valueOf(parameters.get("diversity:maxResultRankingLength"));
      } else {
        outputLength = Integer.valueOf(trecEvalOutputLength);
      }
      // Each pass of the loop processes one query.

      while ((qLine = input.readLine()) != null) {
        int d = qLine.indexOf(':');

        if (d < 0) {
          throw new IllegalArgumentException("Syntax error:  Missing ':' in query line.");
        }

        printMemoryUsage(false);

        String qid = qLine.substring(0, d);
        String query = qLine.substring(d + 1);

        System.out.println("Query " + qLine);
        // System.out.println(qid);

        ScoreList r = null;
        if (parameters.get("fb") == null || parameters.get("fb").equals("false")) {
          r = processQuery(query, model);
        } else {
          r = needExpand(model, query, qid, parameters);
        }

        if (r != null) {
          r.sort();
          printResults(qid, outputLength, outputFile, r);
          System.out.println();
        }
      }
    } catch (IOException ex) {
      ex.printStackTrace();
    } finally {
      input.close();
    }
  }

  private static ScoreList needExpand(RetrievalModel model, String query, String qid, Map<String, String> parameters)
      throws IOException {
    ScoreList r = null;
    if (parameters.get("fbInitialRankingFile") != null) {
      Map<Integer, ScoreList> map = readInitialRankingFile(parameters.get("fbInitialRankingFile"));
      r = map.get(Integer.parseInt(qid));
    } else {
      r = processQuery(query, model);
      r.sort();
    }
    String learnedQ = expandQuery(r, parameters);
    Double weight = Double.parseDouble(parameters.get("fbOrigWeight"));
    writeLearnedQuery(parameters, learnedQ, qid);
    StringBuilder sb = new StringBuilder();
    String defaultOp = model.defaultQrySopName();
    sb.append("#wand ( " + parameters.get("fbOrigWeight") + " " + defaultOp + " ( " + query + " ) "
        + String.valueOf(1 - weight) + " " + learnedQ + ")");
    System.out.println(sb.toString());
    r = processQuery(sb.toString(), model);
    return r;
  }

  static void writeLearnedQuery(Map<String, String> parameters, String output, String queryName) throws IOException {
    File outputfile = new File(parameters.get("fbExpansionQueryFile"));
    FileWriter writer = new FileWriter(outputfile, true);
    writer.write(queryName + ": " + output + "\n");
    writer.close();
  }

  /**
   * Print the query results.
   * 
   * THIS IS NOT THE CORRECT OUTPUT FORMAT. YOU MUST CHANGE THIS METHOD SO THAT IT
   * OUTPUTS IN THE FORMAT SPECIFIED IN THE HOMEWORK PAGE, WHICH IS:
   * 
   * QueryID Q0 DocID Rank Score RunID
   * 
   * @param queryName Original query.
   * @param result    A list of document ids and scores
   * @throws IOException Error accessing the Lucene index.
   */
  static void printResults(String queryName, int outputLength, File outputFile, ScoreList result) throws IOException {
    FileWriter writer = new FileWriter(outputFile, true);
    if (result.size() < 1) {
      System.out.println(queryName + " Q0 dummy 1 0 fubar");
      writer.write(queryName + " Q0 dummy 1 0 fubar\n");
    } else {
      for (int i = 0; i < result.size(); i++) {
        if (i == outputLength) {
          break;
        }

        // System.out.println(queryName + " Q0 " +
        // Idx.getExternalDocid(result.getDocid(i)) + " " + (i + 1) + " "
        // + result.getDocidScore(i) + " fubar");
        writer.write(queryName + " Q0 " + Idx.getExternalDocid(result.getDocid(i)) + " " + (i + 1) + " "
            + result.getDocidScore(i) + " fubar\n");
      }
    }
    writer.close();
  }

  /**
   * Read the specified parameter file, and confirm that the required parameters
   * are present. The parameters are returned in a HashMap. The caller (or its
   * minions) are responsible for processing them.
   * 
   * @return The parameters, in <key, value> format.
   */
  private static Map<String, String> readParameterFile(String parameterFileName) throws IOException {

    Map<String, String> parameters = new HashMap<String, String>();

    File parameterFile = new File(parameterFileName);

    if (!parameterFile.canRead()) {
      throw new IllegalArgumentException("Can't read " + parameterFileName);
    }

    Scanner scan = new Scanner(parameterFile);
    String line = null;
    do {
      line = scan.nextLine();
      String[] pair = line.split("=");
      parameters.put(pair[0].trim(), pair[1].trim());
    } while (scan.hasNext());

    scan.close();

    if (!(parameters.containsKey("indexPath") && parameters.containsKey("queryFilePath")
        && parameters.containsKey("trecEvalOutputPath") && parameters.containsKey("retrievalAlgorithm")
        && parameters.containsKey("trecEvalOutputLength")) && (!parameters.containsKey("diversity"))) {
      throw new IllegalArgumentException("Required parameters were missing from the parameter file.");
    }

    return parameters;
  }

}
