
/**
 *  Copyright (c) 2019, Carnegie Mellon University.  All Rights Reserved.
 */

import java.io.*;

/**
 * The OR operator for all retrieval models.
 */
public class QrySopSum extends QrySop {

  /**
   * Indicates whether the query has a match.
   * 
   * @param r The retrieval model that determines what is a match
   * @return True if the query matches, otherwise false.
   */
  public boolean docIteratorHasMatch(RetrievalModel r) {
    return this.docIteratorHasMatchMin(r);
  }

  /**
   * Get a score for the document that docIteratorHasMatch matched.
   * 
   * @param r The retrieval model that determines how scores are calculated.
   * @return The document score.
   * @throws IOException Error accessing the Lucene index
   */
  public double getScore(RetrievalModel r) throws IOException {

    if (r instanceof RetrievalModelBM25) {
      return this.getScoreBM25(r);
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
  private double getScoreBM25(RetrievalModel r) throws IOException {
    if (docIteratorHasMatchCache()) {
      double sum = 0.0;
      for (int i = 0; i < this.args.size(); i++) {
        Qry q = this.args.get(i);
        int docid = ((Qry) this).docIteratorGetMatch();
        if (q.docIteratorHasMatch(r) && q.docIteratorGetMatch() == docid) {
          sum += ((QrySop) q).getScore(r);
        }
      }
      return sum;
    }
    return 0.0;
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