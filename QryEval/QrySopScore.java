/**
 *  Copyright (c) 2019, Carnegie Mellon University.  All Rights Reserved.
 */

import java.io.*;
import java.lang.IllegalArgumentException;

/**
 *  The SCORE operator for all retrieval models.
 */
public class QrySopScore extends QrySop {

  /**
   *  Document-independent values that should be determined just once.
   *  Some retrieval models have these, some don't.
   */
  
  /**
   *  Indicates whether the query has a match.
   *  @param r The retrieval model that determines what is a match
   *  @return True if the query matches, otherwise false.
   */
  public boolean docIteratorHasMatch (RetrievalModel r) {
    return this.docIteratorHasMatchFirst (r);
  }

  /**
   *  Get a score for the document that docIteratorHasMatch matched.
   *  @param r The retrieval model that determines how scores are calculated.
   *  @return The document score.
   *  @throws IOException Error accessing the Lucene index
   */
  public double getScore (RetrievalModel r) throws IOException {

    if (r instanceof RetrievalModelUnrankedBoolean) {
      return this.getScoreUnrankedBoolean (r);
    } else if (r instanceof RetrievalModelRankedBoolean) {
      return this.getScoreRankedBoolean(r);
    } else if (r instanceof RetrievalModelBM25) { 
      return this.getScoreBM25((RetrievalModelBM25)r);
    } else if (r instanceof RetrievalModelIndri) { 
      return this.getScoreIndri((RetrievalModelIndri)r);
    } else {
      throw new IllegalArgumentException
        (r.getClass().getName() + " doesn't support the SCORE operator.");
    }
  }
  
  public double getDefaultScore(RetrievalModelIndri r, int docid) throws IOException {
    double mu = r.getMu();
    double lambda = r.getLambda();
    Qry q = this.args.get(0);
    double ctf = (double)((QryIop) q).getCtf();
    String field = ((QryIop) q).field;
    int lend = Idx.getFieldLength(field, docid);
    long lenc = Idx.getSumOfFieldLengths(field);
    // System.out.println(ctf);
    double smooth = getSmooth(mu, lambda, lend, lenc, ctf, 0);
    return smooth;
  }
  
  /**
   *  getScore for the Unranked retrieval model.
   *  @param r The retrieval model that determines how scores are calculated.
   *  @return The document score.
   *  @throws IOException Error accessing the Lucene index
   */
  public double getScoreUnrankedBoolean (RetrievalModel r) throws IOException {
    // System.out.println("unranked get score");
    if (! this.docIteratorHasMatchCache()) {
      return 0.0;
    } else {
      return 1.0;
    }
  }

  public double getScoreRankedBoolean (RetrievalModel r) throws IOException {
    if (this.docIteratorHasMatchCache()) {
      Qry q = this.args.get(0);
      int docid = ((QryIop) q).docIteratorGetMatch();
      int score = ((QryIop) q).invertedList.docTf.get(docid);
      return (double)score;
    } else {
      return 0.0;
    }
  }

  public double getScoreBM25 (RetrievalModelBM25 r) throws IOException {
    double k_1 = r.getK1();
    double b = r.getB();
    double k_3 = r.getK3();
    if (this.docIteratorHasMatchCache()) {
      Qry q = this.args.get(0);
      int docid = ((QryIop) q).docIteratorGetMatch();
      long N = Idx.getNumDocs();
      double d = (double)((QryIop) q).invertedList.df;
      double RSJ = calRSJWeight(N, d);
      int tf = ((QryIop) q).invertedList.docTf.get(docid);
      String f = ((QryIop) q).field;
      // System.out.println(f);
      double tfWeights = getTFWeight(b, f, docid, tf, k_1);
      return RSJ * tfWeights;
    }
    return 0.0;
  }

  public double getScoreIndri (RetrievalModelIndri r) throws IOException {
    double mu = r.getMu();
    double lambda = r.getLambda();
    if (this.docIteratorHasMatchCache()) {
      Qry q = this.args.get(0);
      double ctf = (double)((QryIop) q).getCtf();
      int docid = ((QryIop) q).docIteratorGetMatch();
      int tf = ((QryIop) q).docIteratorGetMatchPosting().tf;
      String field = ((QryIop) q).field;
      int lend = Idx.getFieldLength(field, docid);
      long lenc = Idx.getSumOfFieldLengths(field);
      double smooth = getSmooth(mu, lambda, lend, lenc, ctf, tf);
      // System.out.println(smooth);
      return smooth;
    }
    return 0.0;
  }

  private double getSmooth(double mu, double lambda, int lend, Long lenc, double ctf, int tf) throws IOException {
    if (ctf == 0.0) {
      ctf = 0.5;
    }
    double PMLE = ctf / (double)lenc;
    double tmp = (1 - lambda) * (((double)tf + (mu * PMLE)) / ((double)lend + mu));
    double res = tmp + (lambda * PMLE);
    if (res > 1) {
      System.out.println(lambda * PMLE);
    }
    return res;
  }

  private double calRSJWeight(long N, double d) throws IOException {
    double tmp = N - d + 0.5;
    tmp = Math.log(tmp / (d + 0.5));
    return Math.max(0, tmp);
  }

  private double getTFWeight(double b, String f,
                            int docid, int tf,
                            double k_1) throws IOException {
    int doclen = Idx.getFieldLength(f, docid);
    double avglen = Idx.getSumOfFieldLengths(f) / (double)Idx.getDocCount(f);
    double tmp = (1 - b) + ( b * (doclen / avglen));
    double weight = tf / (tf + (k_1 * (tmp)));
    return weight;
  }

  /**
   *  Initialize the query operator (and its arguments), including any
   *  internal iterators.  If the query operator is of type QryIop, it
   *  is fully evaluated, and the results are stored in an internal
   *  inverted list that may be accessed via the internal iterator.
   *  @param r A retrieval model that guides initialization
   *  @throws IOException Error accessing the Lucene index.
   */
  public void initialize (RetrievalModel r) throws IOException {

    Qry q = this.args.get (0);
    q.initialize (r);
  }

}
