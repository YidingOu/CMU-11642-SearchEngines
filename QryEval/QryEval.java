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
 *  This software illustrates the architecture for the portion of a
 *  search engine that evaluates queries.  It is a guide for class
 *  homework assignments, so it emphasizes simplicity over efficiency.
 *  It implements an unranked Boolean retrieval model, however it is
 *  easily extended to other retrieval models.  For more information,
 *  see the ReadMe.txt file.
 */
public class QryEval {

  //  --------------- Constants and variables ---------------------

  private static final String USAGE =
    "Usage:  java QryEval paramFile\n\n";

  private static final String[] TEXT_FIELDS =
    { "body", "title", "url", "inlink" };


  //  --------------- Methods ---------------------------------------

  /**
   *  @param args The only argument is the parameter file name.
   *  @throws Exception Error accessing the Lucene index.
   */
  public static void main(String[] args) throws Exception {

    //  This is a timer that you may find useful.  It is used here to
    //  time how long the entire program takes, but you can move it
    //  around to time specific parts of your code.
    
    Timer timer = new Timer();
    timer.start ();

    //  Check that a parameter file is included, and that the required
    //  parameters are present.  Just store the parameters.  They get
    //  processed later during initialization of different system
    //  components.

    if (args.length < 1) {
      throw new IllegalArgumentException (USAGE);
    }

    Map<String, String> parameters = readParameterFile (args[0]);

    //  Open the index and initialize the retrieval model.

    Idx.open (parameters.get ("indexPath"));
    RetrievalModel model = initializeRetrievalModel (parameters);

    //  Perform experiments.
    
    processQueryFile(parameters.get("queryFilePath"), parameters.get("trecEvalOutputLength"), parameters.get("trecEvalOutputPath"), model);

    //  Clean up.
    
    timer.stop ();
    System.out.println ("Time:  " + timer);
  }

  /**
   *  Allocate the retrieval model and initialize it using parameters
   *  from the parameter file.
   *  @return The initialized retrieval model
   *  @throws IOException Error accessing the Lucene index.
   */
  private static RetrievalModel initializeRetrievalModel (Map<String, String> parameters)
    throws IOException {

    RetrievalModel model = null;
    String modelString = parameters.get ("retrievalAlgorithm").toLowerCase();
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
    }
    else {
      throw new IllegalArgumentException
        ("Unknown retrieval model " + parameters.get("retrievalAlgorithm"));
    }
      
    return model;
  }

  /**
   * Print a message indicating the amount of memory used. The caller can
   * indicate whether garbage collection should be performed, which slows the
   * program but reduces memory usage.
   * 
   * @param gc
   *          If true, run the garbage collector before reporting.
   */
  public static void printMemoryUsage(boolean gc) {

    Runtime runtime = Runtime.getRuntime();

    if (gc)
      runtime.gc();

    System.out.println("Memory used:  "
        + ((runtime.totalMemory() - runtime.freeMemory()) / (1024L * 1024L)) + " MB");
  }

  /**
   * Process one query.
   * @param qString A string that contains a query.
   * @param model The retrieval model determines how matching and scoring is done.
   * @return Search results
   * @throws IOException Error accessing the index
   */
  static ScoreList processQuery(String qString, RetrievalModel model)
    throws IOException {

    String defaultOp = model.defaultQrySopName ();
    // System.out.println("before " + qString);
    qString = defaultOp + "(" + qString + ")";
    // System.out.println("after " + qString);
    Qry q = QryParser.getQuery (qString);
    
    // Show the query that is evaluated
    
    System.out.println("    --> " + q);
    
    if (q != null) {
      // System.out.println(q.getClass().getName());
      ScoreList r = new ScoreList ();
      
      if (q.args.size () > 0) {		// Ignore empty queries

        q.initialize (model);
        
        while (q.docIteratorHasMatch (model)) {
          int docid = q.docIteratorGetMatch ();
          // System.out.println(Idx.getInternalDocid("GX000-48-5866977") );
          //System.out.println(Idx.getExternalDocid(0));
          // if (Idx.getExternalDocid(docid).equals("GX022-93-1111575")) {
          //   System.out.println(docid);
          // }

          //104413  533875
          double score = ((QrySop) q).getScore (model);
          r.add (docid, score);
          q.docIteratorAdvancePast (docid);
          
        }
        
      }

      return r;
    } else
      return null;
  }

  /**
   *  Process the query file.
   *  @param queryFilePath
   *  @param model
   *  @throws IOException Error accessing the Lucene index.
   */
  static void processQueryFile(String queryFilePath,
                               String trecEvalOutputLength,
                               String trecEvalOutputPath,
                               RetrievalModel model)
      throws IOException {

    BufferedReader input = null;

    try {
      String qLine = null;

      File outputFile = new File(trecEvalOutputPath);

      input = new BufferedReader(new FileReader(queryFilePath));

      int outputLength = Integer.valueOf(trecEvalOutputLength);

      //  Each pass of the loop processes one query.

      while ((qLine = input.readLine()) != null) {
        int d = qLine.indexOf(':');

        if (d < 0) {
          throw new IllegalArgumentException
            ("Syntax error:  Missing ':' in query line.");
        }

        printMemoryUsage(false);

        String qid = qLine.substring(0, d);
        String query = qLine.substring(d + 1);

        System.out.println("Query " + qLine);
        // System.out.println(qid);

        ScoreList r = null;

        r = processQuery(query, model);

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

  /**
   * Print the query results.
   * 
   * THIS IS NOT THE CORRECT OUTPUT FORMAT. YOU MUST CHANGE THIS METHOD SO
   * THAT IT OUTPUTS IN THE FORMAT SPECIFIED IN THE HOMEWORK PAGE, WHICH IS:
   * 
   * QueryID Q0 DocID Rank Score RunID
   * 
   * @param queryName
   *          Original query.
   * @param result
   *          A list of document ids and scores
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
        
        // System.out.println(queryName + " Q0 " + Idx.getExternalDocid(result.getDocid(i)) + " " + (i + 1) + " "
        //     + result.getDocidScore(i) + " fubar");
        writer.write(queryName + " Q0 " + Idx.getExternalDocid(result.getDocid(i)) + " " + (i + 1) + " "
        + result.getDocidScore(i) + " fubar\n");
      }
    }
    writer.close();
  }

  /**
   *  Read the specified parameter file, and confirm that the required
   *  parameters are present.  The parameters are returned in a
   *  HashMap.  The caller (or its minions) are responsible for processing
   *  them.
   *  @return The parameters, in <key, value> format.
   */
  private static Map<String, String> readParameterFile (String parameterFileName)
    throws IOException {

    Map<String, String> parameters = new HashMap<String, String>();

    File parameterFile = new File (parameterFileName);

    if (! parameterFile.canRead ()) {
      throw new IllegalArgumentException
        ("Can't read " + parameterFileName);
    }

    Scanner scan = new Scanner(parameterFile);
    String line = null;
    do {
      line = scan.nextLine();
      String[] pair = line.split ("=");
      parameters.put(pair[0].trim(), pair[1].trim());
    } while (scan.hasNext());

    scan.close();

    if (! (parameters.containsKey ("indexPath") &&
           parameters.containsKey ("queryFilePath") &&
           parameters.containsKey ("trecEvalOutputPath") &&
           parameters.containsKey ("retrievalAlgorithm") &&
           parameters.containsKey ("trecEvalOutputLength"))) {
      throw new IllegalArgumentException
        ("Required parameters were missing from the parameter file.");
    }

    return parameters;
  }

}
