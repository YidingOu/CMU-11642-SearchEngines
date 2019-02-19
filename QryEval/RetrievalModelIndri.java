/**
 *  Copyright (c) 2019, Carnegie Mellon University.  All Rights Reserved.
 */

/**
 *  An object that stores parameters for the unranked Boolean
 *  retrieval model (there are none) and indicates to the query
 *  operators how the query should be evaluated.
 */
public class RetrievalModelIndri extends RetrievalModel {
    private double mu;

    private double lambda;

    public RetrievalModelIndri(double mu, double lambda) {
        this.mu = mu;
        this.lambda = lambda;
    }

    public double getMu() {
      return this.mu;
    }

    public double getLambda() {
      return this.lambda;
    }



  public String defaultQrySopName () {
    return new String ("#and");
  }

}
