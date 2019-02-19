
/**
 *  Copyright (c) 2019, Carnegie Mellon University.  All Rights Reserved.
 */

import java.io.*;

/**
 * The OR operator for all retrieval models.
 */
public class QrySopAnd extends QrySop {

  /**
   * Indicates whether the query has a match.
   * 
   * @param r The retrieval model that determines what is a match
   * @return True if the query matches, otherwise false.
   */
  public boolean docIteratorHasMatch(RetrievalModel r) {
    if (r instanceof RetrievalModelIndri) {
      return this.docIteratorHasMatchMin(r);
    } else {
      return this.docIteratorHasMatchAll(r);
    }
  }

  /**
   * Get a score for the document that docIteratorHasMatch matched.
   * 
   * @param r The retrieval model that determines how scores are calculated.
   * @return The document score.
   * @throws IOException Error accessing the Lucene index
   */
  public double getScore(RetrievalModel r) throws IOException {

    if (r instanceof RetrievalModelUnrankedBoolean) {
      return this.getScoreUnrankedBoolean(r);
    } else if (r instanceof RetrievalModelRankedBoolean) {
      return this.getScoreRankedBoolean(r);
    } else if (r instanceof RetrievalModelIndri) {
      return this.getScoreIndri((RetrievalModelIndri) r);
    } else {
      throw new IllegalArgumentException(r.getClass().getName() + " doesn't support the AND operator.");
    }
  }

  /**
   * getScore for the UnrankedBoolean retrieval model.
   * 
   * @param r The retrieval model that determines how scores are calculated.
   * @return The document score.
   * @throws IOException Error accessing the Lucene index
   */
  private double getScoreUnrankedBoolean(RetrievalModel r) throws IOException {
    // System.out.println("AND unranked get score");
    if (!this.docIteratorHasMatchCache()) {
      return 0.0;
    } else {
      return 1.0;
    }
  }

  private double getScoreRankedBoolean(RetrievalModel r) throws IOException {
    if (this.docIteratorHasMatchCache()) {
      // System.out.println("AND ranked get score");
      double min = (double) Integer.MAX_VALUE;
      for (int i = 0; i < this.args.size(); i++) {
        Qry q = this.args.get(i);
        int doc = ((Qry) this).docIteratorGetMatch();
        double score = 0.0;
        if (q.docIteratorHasMatch(r) && q.docIteratorGetMatch() == doc) {
          score = ((QrySop) q).getScore(r);
        }
        min = Math.min(min, score);
      }
      // System.out.println("and: " + min);
      return min;
    } else {
      return 0.0;
    }
  }

  private double getScoreIndri(RetrievalModelIndri r) throws IOException {
    if (this.docIteratorHasMatchCache()) {
      double score = 1.0;
      double size = 1.0 / this.args.size();
      for (int i = 0; i < this.args.size(); i++) {
        Qry q = this.args.get(i);
        int doc = ((Qry) this).docIteratorGetMatch();
        if (q.docIteratorHasMatch(r) && q.docIteratorGetMatch() == doc) {
          double tmp = ((QrySop) q).getScore(r);
          score *= Math.pow(tmp, size);
        } else {
          double tmp = ((QrySop) q).getDefaultScore(r, doc);
          score *= Math.pow(tmp, size);
        }
      } 
      return score;
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
