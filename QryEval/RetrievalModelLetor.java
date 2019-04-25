import java.io.*;
import java.util.*;

/**
 *  Copyright (c) 2019, Carnegie Mellon University.  All Rights Reserved.
 */

/**
 * An object that stores parameters for the unranked Boolean retrieval model
 * (there are none) and indicates to the query operators how the query should be
 * evaluated.
 */
public class RetrievalModelLetor extends RetrievalModel {

    private Map<String, String> parameters;
    private int[] features = new int[18];
    private double[] bm25 = new double[3];
    private double[] indri = new double[2];
    private Map<String, Float> pageRank = new HashMap<>();
    private Map<Integer, Map<String, Double>> relevanceMap = new HashMap<>();
    private Map<Integer, Map<Integer, minMax>> norm = new HashMap<>();

    public RetrievalModelLetor(Map<String, String> parameters) {
        this.parameters = parameters;
        if (parameters.containsKey("letor:featureDisable")) {
            String[] disabled = parameters.get("letor:featureDisable").split(",");
            for (int i = 0; i < disabled.length; i++) {
                features[Integer.parseInt(disabled[i]) - 1] = 1;
            }
        }
        if (parameters.containsKey("BM25:k_1")) {
            bm25[0] = Double.parseDouble(parameters.get("BM25:k_1"));
            bm25[1] = Double.parseDouble(parameters.get("BM25:b"));
            bm25[2] = Double.parseDouble(parameters.get("BM25:k_3"));
        }
        if (parameters.containsKey("Indri:mu")) {
            indri[0] = Double.parseDouble(parameters.get("Indri:mu"));
            indri[1] = Double.parseDouble(parameters.get("Indri:lambda"));
        }
    }

    public void process() throws IOException, Exception {
        String path = this.parameters.get("letor:trainingQrelsFile");
        relevanceFile(path, true);
        processFeatures(true);
        trainSVM();
        RetrievalModelBM25 model = new RetrievalModelBM25(this.bm25[0], this.bm25[1], this.bm25[2]);
        QryEval.processQueryFile(this.parameters.get("queryFilePath"), this.parameters.get("trecEvalOutputLength"),
                this.parameters.get("trecEvalOutputPath"), model, this.parameters);
        relevanceFile(this.parameters.get("trecEvalOutputPath"), false);
        reInitializeOutput();
        processFeatures(false);
        testSVM();
        reRank();

    }

    private void reRank() throws Exception {
        BufferedReader score = new BufferedReader(new FileReader(this.parameters.get("letor:testingDocumentScores")));
        BufferedReader iniRank = new BufferedReader(
                new FileReader(this.parameters.get("letor:testingFeatureVectorsFile")));
        String scoreLine = null;
        String iniLine = null;
        Map<Integer, List<reRankOrderObj>> cacheMap = new HashMap<>();

        while (((scoreLine = score.readLine()) != null) && ((iniLine = iniRank.readLine()) != null)) {
            scoreLine.trim();
            iniLine.trim();
            String[] parts = iniLine.split(" ");
            String[] rawQid = parts[1].trim().split(":");
            int qid = Integer.parseInt(rawQid[1].trim());
            String externalId = parts[parts.length - 1].trim();
            if (scoreLine.equals("nan")) {
                scoreLine = "0.0";
            }
            double relScore = Double.parseDouble(scoreLine);
            int docid = Idx.getInternalDocid(externalId);
            reRankOrderObj obj = new reRankOrderObj(docid, relScore);
            if (!cacheMap.containsKey(qid)) {
                cacheMap.put(qid, new ArrayList<reRankOrderObj>());
            }
            cacheMap.get(qid).add(obj);
        }
        List<Integer> sortedLst = new ArrayList<>(cacheMap.keySet());
        Collections.sort(sortedLst);
        for (int qryID : sortedLst) {
            List<reRankOrderObj> rerank = cacheMap.get(qryID);
            ScoreList r = new ScoreList();
            for (reRankOrderObj tuple : rerank) {
                r.add(tuple.docid, tuple.score);
            }
            r.sort();
            printResult(qryID, r);
        }
        score.close();
        iniRank.close();
    }

    private void printResult(int qid, ScoreList result) throws IOException {
        FileWriter writer = new FileWriter(this.parameters.get("trecEvalOutputPath"), true);
        int outputLength = Integer.parseInt(this.parameters.get("trecEvalOutputLength"));
        String queryName = String.valueOf(qid);
        if (result.size() < 1) {
            System.out.println(queryName + " Q0 dummy 1 0 fubar");
            writer.write(queryName + " Q0 dummy 1 0 fubar\n");
        } else {
            for (int i = 0; i < result.size(); i++) {
                if (i == outputLength) {
                    break;
                }

                // System.out.println(queryName + " Q0 " +
                // Idx.getExternalDocid(result.getDocid(i)) + " " + (i + 1) + " "
                // + result.getDocidScore(i) + " fubar");
                writer.write(queryName + " Q0 " + Idx.getExternalDocid(result.getDocid(i)) + " " + (i + 1) + " "
                        + result.getDocidScore(i) + " fubar\n");
            }
        }
        writer.close();
    }

    private void testSVM() throws Exception {
        // runs svm_rank_learn from within Java to train the model
        // execPath is the location of the svm_rank_learn utility,
        // which is specified by letor:svmRankLearnPath in the parameter file.
        // FEAT_GEN.c is the value of the letor:c parameter.
        String execPath = this.parameters.get("letor:svmRankClassifyPath");
        String qrelsFeatureOutputFile = this.parameters.get("letor:testingFeatureVectorsFile");
        String input = this.parameters.get("letor:svmRankModelFile");
        String score = this.parameters.get("letor:testingDocumentScores");
        Process cmdProc = Runtime.getRuntime().exec(new String[] { execPath, qrelsFeatureOutputFile, input, score });

        // The stdout/stderr consuming code MUST be included.
        // It prevents the OS from running out of output buffer space and stalling.

        // consume stdout and print it out for debugging purposes
        BufferedReader stdoutReader = new BufferedReader(new InputStreamReader(cmdProc.getInputStream()));
        String line;
        while ((line = stdoutReader.readLine()) != null) {
            System.out.println(line);
        }
        // consume stderr and print it for debugging purposes
        BufferedReader stderrReader = new BufferedReader(new InputStreamReader(cmdProc.getErrorStream()));
        while ((line = stderrReader.readLine()) != null) {
            System.out.println(line);
        }

        // get the return value from the executable. 0 means success, non-zero
        // indicates a problem
        int retValue = cmdProc.waitFor();
        if (retValue != 0) {
            throw new Exception("SVM Rank crashed.");
        }
    }

    private void trainSVM() throws Exception {
        String qrelsFeatureOutputFile = this.parameters.get("letor:trainingFeatureVectorsFile");
        String path = this.parameters.get("letor:svmRankLearnPath");
        String ouput = this.parameters.get("letor:svmRankModelFile");
        String gen = this.parameters.get("letor:svmRankParamC");
        Process cmdProc = Runtime.getRuntime().exec(new String[] { path, "-c", gen, qrelsFeatureOutputFile, ouput });
        // The stdout/stderr consuming code MUST be included.
        // It prevents the OS from running out of output buffer space and stalling.

        // consume stdout and print it out for debugging purposes
        BufferedReader stdoutReader = new BufferedReader(new InputStreamReader(cmdProc.getInputStream()));
        String line;
        while ((line = stdoutReader.readLine()) != null) {
            System.out.println(line);
        }
        // consume stderr and print it for debugging purposes
        BufferedReader stderrReader = new BufferedReader(new InputStreamReader(cmdProc.getErrorStream()));
        while ((line = stderrReader.readLine()) != null) {
            System.out.println(line);
        }

        // get the return value from the executable. 0 means success, non-zero
        // indicates a problem
        int retValue = cmdProc.waitFor();
        if (retValue != 0) {
            throw new Exception("SVM Rank crashed.");
        }
    }

    private void relevanceFile(String path, boolean flag) throws IOException {
        this.relevanceMap = new HashMap<>();
        BufferedReader reader = new BufferedReader(new FileReader(path));
        String qLine = null;
        if (flag) {
            while ((qLine = reader.readLine()) != null) {
                qLine.trim();
                String[] cols = qLine.split(" ");
                int qid = Integer.parseInt(cols[0].trim());
                String docid = cols[2].trim();
                double degree = Double.parseDouble(cols[3].trim());
                Map<String, Double> map = this.relevanceMap.get(qid);
                if (map == null) {
                    Map<String, Double> tmp = new HashMap<>();
                    tmp.put(docid, degree);
                    this.relevanceMap.put(qid, tmp);
                } else {
                    map.put(docid, degree);
                }
            }
        } else {
            while ((qLine = reader.readLine()) != null) {
                qLine.trim();
                String[] cols = qLine.split(" ");
                int qid = Integer.parseInt(cols[0].trim());
                String docid = cols[2].trim();
                double degree = Double.parseDouble(cols[4].trim());

                Map<String, Double> map = this.relevanceMap.get(qid);
                if (map == null) {
                    Map<String, Double> tmp = new HashMap<>();
                    tmp.put(docid, degree);
                    this.relevanceMap.put(qid, tmp);
                } else {
                    map.put(docid, degree);
                }
            }
        }
        reader.close();
    }

    private void reInitializeOutput() throws Exception {
        BufferedWriter writer = new BufferedWriter(new FileWriter(this.parameters.get("trecEvalOutputPath"), false));
        writer.close();
    }

    private Map<Integer, String> processQuery(String path) throws IOException {
        Map<Integer, String> map = new HashMap<>();
        BufferedReader reader = new BufferedReader(new FileReader(path));
        String qLine = null;
        while ((qLine = reader.readLine()) != null) {
            int d = qLine.indexOf(':');
            if (d < 0) {
                throw new IllegalArgumentException("Syntax error:  Missing ':' in query line.");
            }
            int qid = Integer.parseInt(qLine.substring(0, d));
            String query = qLine.substring(d + 1);
            map.put(qid, query);
        }
        reader.close();
        return map;
    }

    private void processFeatures(Boolean flag) throws Exception {
        Map<Integer, String> qry = new HashMap<>();

        if (flag) {
            qry = processQuery(this.parameters.get("letor:trainingQueryFile"));
        } else {
            qry = processQuery(this.parameters.get("queryFilePath"));
        }
        Map<String, Map<Integer, Double>> docFeature = new HashMap<>();
        List<Integer> sort = new ArrayList<>(qry.keySet());
        Collections.sort(sort);

        for (int qid : sort) {
            // System.out.println(qid);
            Map<Integer, minMax> tmp = initialMinMax();
            this.norm.put(qid, tmp);
            String q = qry.get(qid);
            String[] terms = QryParser.tokenizeString(q);
            for (Map.Entry<String, Double> entry : this.relevanceMap.get(qid).entrySet()) {
                Map<Integer, Double> map = new HashMap<>();
                int docid;
                try {
                    docid = Idx.getInternalDocid(entry.getKey());
                } catch (Exception e) {
                    continue;
                }
                TermVector tv_body = new TermVector(docid, "body");
                TermVector tv_title = new TermVector(docid, "title");
                TermVector tv_url = new TermVector(docid, "url");
                TermVector tv_inlink = new TermVector(docid, "inlink");

                // System.out.println(docid);
                // f1
                if (this.features[0] == 0) {
                    Double spamScore = Double.MIN_VALUE;
                    spamScore = Double.parseDouble(Idx.getAttribute("spamScore", docid));
                    // System.out.println(spamScore);
                    map.put(1, spamScore);
                    buildNorm(qid, 1, spamScore);
                }
                // f2
                if (this.features[1] == 0) {
                    String rawUrl = Idx.getAttribute("rawUrl", docid).replace("http://", "");
                    double count = 0.0;
                    for (int i = 0; i < rawUrl.length(); i++) {
                        if (rawUrl.charAt(i) == '/') {
                            count++;
                        }
                    }
                    map.put(2, count);
                    buildNorm(qid, 2, count);
                }
                // f3
                if (this.features[2] == 0) {
                    String rawUrl = Idx.getAttribute("rawUrl", docid);
                    double contains = 0.0;
                    if (rawUrl.contains("wikipedia.org"))
                        contains = 1.0;
                    map.put(3, contains);
                    buildNorm(qid, 3, contains);
                }
                // f4
                if (this.features[3] == 0) {
                    double prScore = Double.parseDouble(Idx.getAttribute("PageRank", docid));
                    map.put(4, prScore);
                    buildNorm(qid, 4, prScore);
                }
                // f5
                if (this.features[4] == 0) {
                    double score = getScoreBM25(terms, docid, "body", tv_body);
                    map.put(5, score);
                    buildNorm(qid, 5, score);
                }
                // f6
                if (this.features[5] == 0) {
                    double score = getScoreIndri(terms, "body", docid, tv_body);
                    map.put(6, score);
                    buildNorm(qid, 6, score);
                }
                // f7
                if (this.features[6] == 0) {
                    double score = getTermOverlap(terms, docid, "body", tv_body);
                    map.put(7, score);
                    buildNorm(qid, 7, score);
                }
                // f8
                if (this.features[7] == 0) {
                    double score = getScoreBM25(terms, docid, "title", tv_title);
                    map.put(8, score);
                    buildNorm(qid, 8, score);
                }
                // f9
                if (this.features[8] == 0) {
                    double score = getScoreIndri(terms, "title", docid, tv_title);
                    map.put(9, score);
                    buildNorm(qid, 9, score);
                }
                // f10
                if (this.features[9] == 0) {
                    double score = getTermOverlap(terms, docid, "title", tv_title);
                    map.put(10, score);
                    buildNorm(qid, 10, score);
                }
                // f11
                if (this.features[10] == 0) {
                    double score = getScoreBM25(terms, docid, "url", tv_url);
                    map.put(11, score);
                    buildNorm(qid, 11, score);
                }
                // f12
                if (this.features[11] == 0) {
                    double score = getScoreIndri(terms, "url", docid, tv_url);
                    map.put(12, score);
                    buildNorm(qid, 12, score);
                }
                // f13
                if (this.features[12] == 0) {
                    double score = getTermOverlap(terms, docid, "url", tv_url);
                    map.put(13, score);
                    buildNorm(qid, 13, score);
                }
                // f14
                if (this.features[13] == 0) {
                    double score = getScoreBM25(terms, docid, "inlink", tv_inlink);
                    map.put(14, score);
                    buildNorm(qid, 14, score);

                }
                // f15
                if (this.features[14] == 0) {
                    double score = getScoreIndri(terms, "inlink", docid, tv_inlink);
                    map.put(15, score);
                    buildNorm(qid, 15, score);
                }
                // f16
                if (this.features[15] == 0) {
                    double score = getTermOverlap(terms, docid, "inlink", tv_inlink);
                    map.put(16, score);
                    buildNorm(qid, 16, score);
                }
                // f17
                if (this.features[16] == 0) {
                    double inLink = (double) Idx.getFieldLength("inlink", docid);
                    map.put(17, inLink);
                    buildNorm(qid, 17, inLink);
                }
                // f18
                if (this.features[17] == 0) {
                    double titleLen = (double) Idx.getFieldLength("title", docid);
                    map.put(18, titleLen);
                    buildNorm(qid, 18, titleLen);
                }
                docFeature.put(entry.getKey(), map);
            }
            if (flag) {
                normFeature(this.parameters.get("letor:trainingFeatureVectorsFile"), docFeature, qid);
            } else {
                normFeature(this.parameters.get("letor:testingFeatureVectorsFile"), docFeature, qid);
            }
        }
    }

    private void normFeature(String path, Map<String, Map<Integer, Double>> extFeat, int qid) throws IOException {
        FileWriter writer = new FileWriter(path, true);
        Map<String, Double> relDegree = this.relevanceMap.get(qid);
        for (Map.Entry<String, Map<Integer, Double>> entries : extFeat.entrySet()) {
            String eid = entries.getKey();
            Map<Integer, Double> featScore = entries.getValue();
            double rele = relDegree.containsKey(eid) ? relDegree.get(eid) : 0;
            StringBuilder sb = new StringBuilder();
            sb.append(rele + " qid:" + qid + " ");
            for (int i = 0; i < 18; i++) {
                if (this.features[i] == 0) {
                    int index = i + 1;
                    double min = this.norm.get(qid).get(index).min;
                    double max = this.norm.get(qid).get(index).max;
                    // if (min == Double.MAX_VALUE || max == Double.MIN_VALUE) {
                    // continue;
                    // }
                    double score = featScore.get(index);
                    double out = 0.0;
                    if (score != Double.MIN_VALUE) {
                        if (max == min) {
                            out = 0.0;
                        } else {
                            out = (score - min) / (max - min);
                        }
                        // double tmp = score - min;
                        // double tmp2 = max - min;
                        // System.out.println(out);
                    }
                    sb.append(index + ":" + out + " ");
                }
            }
            sb.append("# " + eid + "\n");
            writer.write(sb.toString());
        }

        writer.close();
    }

    private Map<Integer, minMax> initialMinMax() throws IOException {
        Map<Integer, minMax> results = new HashMap<>();
        for (int i = 0; i < 18; i++) {
            if (features[i] == 0) {
                results.put(i + 1, new minMax(Double.MAX_VALUE, Double.MIN_VALUE));
            }
        }
        return results;
    }

    private void buildNorm(int qid, int feature, double score) throws IOException {
        // System.out.println(qid + " " + feature);
        if (score == Double.MIN_VALUE) {
            return;
        }
        Map<Integer, minMax> tmp = this.norm.get(qid);
        tmp.get(feature).max = Math.max(tmp.get(feature).max, score);
        tmp.get(feature).min = Math.min(tmp.get(feature).min, score);
        // System.out.println(qid + " " + feature + " " + tmp.get(feature).min);
    }

    public String defaultQrySopName() {
        return new String(" ");
    }

    private double getScoreBM25(String[] terms, int docid, String field, TermVector tv) throws IOException {
        double score = 0.0;
        long N = Idx.getNumDocs();
        if (tv.stemsLength() == 0) {
            return Double.MIN_VALUE;
        }
        for (String term : terms) {
            int index = tv.indexOfStem(term);
            if (index == -1) {
                continue;
            }
            double df = (double) tv.stemDf(index);
            double tf = (double) tv.stemFreq(index);
            double RSJ = calRSJWeight(N, df);
            double tfWeights = getTFWeight(field, docid, tf);
            score = score + (RSJ * tfWeights);
        }
        return score;

    }

    private double calRSJWeight(long N, double d) throws IOException {
        double tmp = N - d + 0.5;
        tmp = Math.log(tmp / (d + 0.5));
        return Math.max(0, tmp);
    }

    private double getTFWeight(String f, int docid, double tf) throws IOException {
        int doclen = Idx.getFieldLength(f, docid);
        double avglen = Idx.getSumOfFieldLengths(f) / (double) Idx.getDocCount(f);
        double tmp = (1 - this.bm25[1]) + (this.bm25[1] * (doclen / avglen));
        double weight = tf / (tf + (this.bm25[0] * (tmp)));
        return weight;
    }

    private double getScoreIndri(String[] terms, String field, int docid, TermVector tv) throws IOException {
        int count = 0;
        double score = 1.0;
        if (tv.stemsLength() == 0) {
            return Double.MIN_VALUE;
        }
        for (String term : terms) {
            int index = tv.indexOfStem(term);
            double tf = 0.0;
            double ctf = Idx.getTotalTermFreq(field, term);
            if (index != -1) {
                tf = tv.stemFreq(index);
                count++;
            }
            int lend = Idx.getFieldLength(field, docid);
            long lenc = Idx.getSumOfFieldLengths(field);
            double smooth = getSmooth(lend, lenc, ctf, tf);
            // System.out.println(smooth);
            score *= smooth;
        }
        double tmp = (double) 1 / terms.length;
        return count != 0 ? Math.pow(score, tmp) : 0.0;
    }

    private double getSmooth(int lend, Long lenc, double ctf, double tf) throws IOException {
        if (ctf == 0.0) {
            ctf = 0.5;
        }
        double PMLE = ctf / (double) lenc;
        double tmp = (1 - this.indri[1]) * ((tf + (this.indri[0] * PMLE)) / ((double) lend + this.indri[0]));
        double res = tmp + (this.indri[1] * PMLE);
        if (res > 1) {
            System.out.println(this.indri[1] * PMLE);
        }
        return res;
    }

    private double getTermOverlap(String[] terms, int docid, String field, TermVector tv) throws IOException {
        int count = 0;
        if (tv.stemsLength() == 0) {
            return Double.MIN_VALUE;
        }
        for (String term : terms) {
            int index = tv.indexOfStem(term);
            if (index != -1) {
                count++;
            }
        }
        return (double) count / terms.length;

    }

}

class minMax {
    public double min;
    public double max;

    public minMax(double min, double max) {
        this.min = min;
        this.max = max;
    }
}

class reRankOrderObj {

    public int docid;
    public double score;

    public reRankOrderObj(int docid, double score) {
        this.docid = docid;
        this.score = score;
    }
}
