
/**
 *  Copyright (c) 2019, Carnegie Mellon University.  All Rights Reserved.
 */
import java.io.*;
import java.util.*;

/**
 * The Near operator for all retrieval models.
 */
public class QryIopNear extends QryIop {

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

        int lcoCount = 0;
        boolean needBreak = false;
        boolean restart = false;

        for (int i = 0; i < this.args.size(); i++) {
          if (restart) {
            i = 0;
            restart = false;
          }

          Qry q = this.args.get(i);

          if (((QryIop) q).locIteratorHasMatch()) {

            int locid = ((QryIop) q).locIteratorGetMatch();

            if (i != 0) {
              Qry prev = this.args.get(i - 1);
              int previd = ((QryIop) prev).locIteratorGetMatch();

              if (locid < previd) {

                ((QryIop) q).locIteratorAdvancePast(previd);

                if (!((QryIop) q).locIteratorHasMatch()) {
                  needBreak = true;
                  break;
                }
                locid = ((QryIop) q).locIteratorGetMatch();
              } else {

                if (locid - previd <= this.distance) {
                  lcoCount++;
                } else {
                  ((QryIop) this.args.get(0)).locIteratorAdvance();
                  restart = true;
                }
              }
            }
          } else {
            needBreak = true;
            break;
          }
        }

        if (lcoCount == this.args.size() - 1) {
          Qry q = this.args.get(this.args.size() - 1);
          int lid = ((QryIop) q).locIteratorGetMatch();
          if (!results.containsKey(docid)) {
            results.put(docid, new ArrayList<Integer>());
          }
          results.get(docid).add(lid);
          for (Qry qi : this.args) {
            ((QryIop) qi).locIteratorAdvance();
          }
        }

        if (needBreak) {
          break;
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
