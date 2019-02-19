/**
 *  Copyright (c) 2019, Carnegie Mellon University.  All Rights Reserved.
 */

import java.io.*;

/**
 *  The OR operator for all retrieval models.
 */
public class QrySopOr extends QrySop {

  /**
   *  Indicates whether the query has a match.
   *  @param r The retrieval model that determines what is a match
   *  @return True if the query matches, otherwise false.
   */
  public boolean docIteratorHasMatch (RetrievalModel r) {
    return this.docIteratorHasMatchMin (r);
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
    } else {
      throw new IllegalArgumentException
        (r.getClass().getName() + " doesn't support the OR operator.");
    }
  }
  
  /**
   *  getScore for the UnrankedBoolean retrieval model.
   *  @param r The retrieval model that determines how scores are calculated.
   *  @return The document score.
   *  @throws IOException Error accessing the Lucene index
   */
  private double getScoreUnrankedBoolean (RetrievalModel r) throws IOException {
    if (! this.docIteratorHasMatchCache()) {
      return 0.0;
    } else {
      return 1.0;
    }
  }

  private double getScoreRankedBoolean (RetrievalModel r) throws IOException {
    if (this.docIteratorHasMatchCache()) {
      double max = 0.0;
      for (int i = 0; i < this.args.size(); i++) {
        Qry q = this.args.get(i);
        int doc = ((Qry) this).docIteratorGetMatch(); 
        double score = 0.0;
        if (q.docIteratorHasMatch(r) && q.docIteratorGetMatch() == doc) {
          score = ((QrySop) q).getScore(r);
        }
        max = Math.max(max, score); 
      }
      return max;
    } else {
      return 0.0;
    }
  }

  public double getDefaultScore(RetrievalModelIndri r, int docid) throws IOException {
    double score = 1.0;
    double size = 1.0 / this.args.size();
    for (int i = 0; i < this.args.size(); i++) {
      Qry q = this.args.get(i);
      double tmp = ((QrySop) q).getDefaultScore(r, docid);
      score *= Math.pow(tmp, size);
    }
    return score;
  }

}
