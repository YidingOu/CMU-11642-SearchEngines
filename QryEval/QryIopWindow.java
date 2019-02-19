
/**
 *  Copyright (c) 2019, Carnegie Mellon University.  All Rights Reserved.
 */
import java.io.*;
import java.util.*;

/**
 * The Near operator for all retrieval models.
 */
public class QryIopWindow extends QryIop {

  protected int distance;

  /**
   * Evaluate the query operator; the result is an internal inverted list that may
   * be accessed via the internal iterators.
   * 
   * @throws IOException Error accessing the Lucene index.
   */
  protected void evaluate() throws IOException {

    // Create an empty inverted list. If there are no query arguments,
    // that's the final result.

    this.invertedList = new InvList(this.getField());

    if (args.size() == 0) {
      return;
    }

    while (this.docIteratorHasMatchAll(null)) {

      Qry qry = this.args.get(0);

      int docid = ((QryIop) qry).docIteratorGetMatch();

      Map<Integer, List<Integer>> results = new HashMap<>();

      while (((QryIop) qry).locIteratorHasMatch()) {

        int minq = 0;
        int maxq = 0;

        int min = Integer.MAX_VALUE;
        int max = Integer.MIN_VALUE;

        boolean needBreak = false;

        for (int i = 0; i < this.args.size(); i++) {
          Qry q = this.args.get(i);
          if (((QryIop) q).locIteratorHasMatch()) {
            int locid = ((QryIop) q).locIteratorGetMatch();
            if (locid < min) {
              minq = i;
              min = locid;
              // System.out.println("min: " + min);
            }
            if (locid > max) {
              maxq = i;
              max = locid;
              // System.out.println("max: " + max);
            }
          } else {
            needBreak = true;
            break;
          }
        }

        if (needBreak)
          break;

        if (max - min < distance) {
          if (!results.containsKey(docid)) {
            results.put(docid, new ArrayList<Integer>());
          }
          results.get(docid).add(max);
          for (Qry qi : this.args) {
            ((QryIop) qi).locIteratorAdvance();
          }
        } else {
          ((QryIop) this.args.get(minq)).locIteratorAdvance();
        }
      }

      for (Map.Entry<Integer, List<Integer>> entries : results.entrySet()) {
        this.invertedList.appendPosting(entries.getKey(), entries.getValue());
      }

      for (Qry qi : this.args) {
        qi.docIteratorAdvancePast(docid);
      }
    }
  }
}
