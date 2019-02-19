
/**
 *  Copyright (c) 2019, Carnegie Mellon University.  All Rights Reserved.
 */

import java.io.*;
import java.util.*;
/**
 * The OR operator for all retrieval models.
 */
public class QrySopWand extends QrySop {

    protected List<Double> weights = new ArrayList<>();

    protected double weight_sum;

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

        if (r instanceof RetrievalModelIndri) {
          return this.getScoreIndri((RetrievalModelIndri)r);
        } else {
        throw new IllegalArgumentException(r.getClass().getName() + " doesn't support the OR operator.");
      }
  }

  /**
   * getScore for the UnrankedBoolean retrieval model.
   * 
   * @param r The retrieval model that determines how scores are calculated.
   * @return The document score.
   * @throws IOException Error accessing the Lucene index
   */

  private double getScoreIndri(RetrievalModelIndri r) throws IOException {
    if (this.docIteratorHasMatchCache()) {
      double score = 1.0;
      for (int i = 0; i < this.args.size(); i++) {
        Qry q = this.args.get(i);
        double pow = weights.get(i) / weight_sum;
        int doc = ((Qry) this).docIteratorGetMatch();
        if (q.docIteratorHasMatch(r) && q.docIteratorGetMatch() == doc) {
          double tmp = ((QrySop) q).getScore(r);
          score *= Math.pow(tmp, pow);
        } else {
          double tmp = ((QrySop) q).getDefaultScore(r, doc);
          score *= Math.pow(tmp, pow);
        }
      } 
      return score;
    } else {
      return 0.0;
    }
  }

  public double getDefaultScore(RetrievalModelIndri r, int docid) throws IOException {
    double score = 1.0;
    for (int i = 0; i < this.args.size(); i++) {
      Qry q = this.args.get(i);
      double pow = weights.get(i) / weight_sum;
      double tmp = ((QrySop) q).getDefaultScore(r, docid);
      score *= Math.pow(tmp, pow);
    }
    return score;
  }
}
