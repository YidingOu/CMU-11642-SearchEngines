/**
 *  Copyright (c) 2019, Carnegie Mellon University.  All Rights Reserved.
 */

/**
 *  An object that stores parameters for the unranked Boolean
 *  retrieval model (there are none) and indicates to the query
 *  operators how the query should be evaluated.
 */
public class RetrievalModelBM25 extends RetrievalModel {

    private double k_1;
    
    private double b;

    private double k_3;



    public String defaultQrySopName () {
      return new String ("#sum");
    }

    public RetrievalModelBM25(double k_1, double b, double k_3) {
        this.k_1 = k_1;
        this.b = b;
        this.k_3 = k_3;
    }

    public double getK1() {
        return this.k_1;
    }

    public double getB() {
        return this.b;
    }

    public double getK3() {
        return this.k_3;
    }
  
  }
  