import java.io.*;
import java.util.*;

public class Diversification {

    private Map<String, String> parameters;

    private Map<Integer, ScoreList> iniRankingCache = new HashMap<>(); // qid -> score

    private Map<Integer, Map<Integer, ScoreList>> iniIntentCahce = new HashMap<>(); // qid -> (intent id -> score)

    private Map<Integer, Map<Integer, String>> iniFileCache = new HashMap<>(); // qid -> (intent id -> Token)

    private Map<Integer, Map<Integer, List<IdScore>>> qryScore = new HashMap<>(); // qid -> (intent / qid -> normed
    // score)
    private Map<Integer, ScoreList> processedRankingCache = new HashMap<>(); // qid -> score

    private Map<Integer, Map<Integer, ScoreList>> processedIntentCache = new HashMap<>();

    private Map<Integer, Integer> qidIntent = new HashMap<>(); // qid -> number of intents

    public Diversification(Map<String, String> parameters) {
        this.parameters = parameters;
    }

    public void process(RetrievalModel model) throws Exception {
        if (this.parameters.containsKey("diversity:initialRankingFile")) {
            readRelevanceFile();
            normScore(true);
            runAlgorithm();
        } else {
            cacheIntentFile();
            processQuery(model);
            normScore(false);
            runAlgorithm();
        }

    }

    private void readRelevanceFile() throws Exception {
        BufferedReader reader = null;
        try {
            String path = this.parameters.get("diversity:initialRankingFile");
            reader = new BufferedReader(new FileReader(path));
            String line = null;
            while ((line = reader.readLine()) != null) {
                String[] parts = line.split(" ");
                String id = parts[0].trim();
                int docid = Idx.getInternalDocid(parts[2].trim());
                if (id.contains(".")) {
                    int index = parts[0].indexOf(".");
                    int qid = Integer.valueOf(parts[0].substring(0, index));
                    int intent = Integer.valueOf(parts[0].substring(index + 1));
                    double score = Double.parseDouble(parts[4].trim());
                    if (!this.iniIntentCahce.containsKey(qid)) {
                        this.iniIntentCahce.put(qid, new HashMap<Integer, ScoreList>());
                    }

                    if (!this.iniIntentCahce.get(qid).containsKey(intent)) {
                        this.iniIntentCahce.get(qid).put(intent, new ScoreList());
                    }

                    this.iniIntentCahce.get(qid).get(intent).add(docid, score);
                    this.iniRankingCache.get(qid).add(docid, score);
                } else {
                    int qid = Integer.valueOf(id);
                    double score = Double.parseDouble(parts[4].trim());
                    if (!this.iniRankingCache.containsKey(qid)) {
                        this.iniRankingCache.put(qid, new ScoreList());
                    }
                    this.iniRankingCache.get(qid).add(docid, score);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            reader.close();
        }
    }

    private void cacheIntentFile() throws IOException {
        BufferedReader reader = null;
        try {
            String path = this.parameters.get("diversity:intentsFile");
            reader = new BufferedReader(new FileReader(path));
            String line = null;
            while ((line = reader.readLine()) != null) {
                String[] parts = line.split(":");
                int index = parts[0].indexOf(".");
                int qid = Integer.valueOf(parts[0].substring(0, index));
                int intent = Integer.valueOf(parts[0].substring(index + 1));
                String qry = parts[1].trim();

                if (!this.iniFileCache.containsKey(qid)) {
                    this.iniFileCache.put(qid, new HashMap<Integer, String>());
                    // System.out.println(qid);
                }
                this.iniFileCache.get(qid).put(intent, qry);
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            reader.close();
        }
    }

    private void processQuery(RetrievalModel model) throws IOException {
        BufferedReader reader = null;
        try {
            String path = this.parameters.get("queryFilePath");
            reader = new BufferedReader(new FileReader(path));
            String line = null;
            while ((line = reader.readLine()) != null) {
                int d = line.indexOf(':');

                if (d < 0) {
                    throw new IllegalArgumentException("Syntax error:  Missing ':' in query line.");
                }
                String qid = line.substring(0, d);
                String query = line.substring(d + 1);

                System.out.println("Query " + line);
                ScoreList r = new ScoreList();

                r = QryEval.processQuery(query, model);
                r.sort();
                r.truncate(Integer.valueOf(this.parameters.get("diversity:maxInputRankingsLength")));
                int id = Integer.valueOf(qid);

                this.processedRankingCache.put(id, r); // each query has unique id, so i dont need to check duplicate
                // for (int idd : this.iniFileCache.keySet()) {
                // System.out.println(this.iniFileCache.get(157).size());
                // System.out.println(qid);
                // }

                if (!this.processedIntentCache.containsKey(id)) {
                    this.processedIntentCache.put(id, new HashMap<Integer, ScoreList>());
                }
                // System.out.println(this.iniFileCache.get(qid));
                for (Map.Entry<Integer, String> tmp : this.iniFileCache.get(id).entrySet()) {
                    int intent = tmp.getKey();
                    String term = tmp.getValue();
                    ScoreList slst = QryEval.processQuery(term, model);
                    slst.sort();
                    slst.truncate(Integer.valueOf(this.parameters.get("diversity:maxInputRankingsLength")));
                    this.processedIntentCache.get(id).put(intent, slst);
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            reader.close();
        }
    }

    private void normScore(boolean flag) {
        Map<Integer, ScoreList> qidScore;
        Map<Integer, Map<Integer, ScoreList>> intentScore;
        boolean needNorm = false;

        if (flag) {
            qidScore = this.iniRankingCache;
            intentScore = this.iniIntentCahce;
        } else {
            qidScore = this.processedRankingCache;
            intentScore = this.processedIntentCache;
        }
        for (int qid : qidScore.keySet()) {
            if (!this.qryScore.containsKey(qid)) {
                this.qryScore.put(qid, new HashMap<Integer, List<IdScore>>());
            }

            ScoreList qScore = qidScore.get(qid);
            Map<Integer, ScoreList> iScore = intentScore.get(qid);
            this.qidIntent.put(qid, iScore.size());
            Double[] max = new Double[iScore.size() + 1];
            Arrays.fill(max, 0.0);
            for (int i = 0; i < qScore.size(); i++) {
                double score = qScore.getDocidScore(i);
                int docid = qScore.getDocid(i);
                if (!this.qryScore.get(qid).containsKey(docid)) {
                    this.qryScore.get(qid).put(docid, new ArrayList<IdScore>());
                }
                this.qryScore.get(qid).get(docid).add(new IdScore(0, score));
                // System.out.println(max[0]);
                max[0] += score;
                if (score > 1) {
                    needNorm = true;
                }
            }
            int count = 1;
            for (int intent : iScore.keySet()) {
                ScoreList slst = iScore.get(intent);
                for (int i = 0; i < slst.size(); i++) {
                    double score = slst.getDocidScore(i);
                    int docid = slst.getDocid(i);
                    if (!this.qryScore.get(qid).containsKey(docid)) {
                        this.qryScore.get(qid).put(docid, new ArrayList<IdScore>());
                    }
                    this.qryScore.get(qid).get(docid).add(new IdScore(intent, score));

                    if (score > 1) {
                        needNorm = true;
                    }
                    max[count] += score;
                }
                count++;
            }

            if (needNorm) {

                List<Double> calMax = Arrays.asList(max);
                double normMax = Collections.max(calMax);

                for (Map.Entry<Integer, List<IdScore>> entry : this.qryScore.get(qid).entrySet()) {
                    for (IdScore tmp : entry.getValue()) {
                        tmp.score = tmp.score / normMax;
                    }
                }
            }
        }

    }

    private void runAlgorithm() throws IOException {
        String algorithm = this.parameters.get("diversity:algorithm");
        algorithm = algorithm.trim().toLowerCase();
        if (algorithm.equals("pm2")) {
            runPM2();
        } else if (algorithm.equals("xquad")) {
            runXquad();
        } else {
            throw new IllegalArgumentException(
                    "Unknown diversification algorithm " + parameters.get("diversity:algorithm"));
        }
    }

    private void runPM2() throws IOException {
        int outputLength = Integer.valueOf(this.parameters.get("diversity:maxResultRankingLength"));
        double lambda = Double.parseDouble(this.parameters.get("diversity:lambda"));
        List<Integer> sortedQid = new ArrayList<>(this.qryScore.keySet());
        Collections.sort(sortedQid);
        for (int qid : sortedQid) {
            ScoreList r = new ScoreList();
            int intentNums = this.qidIntent.get(qid);
            Set<Integer> docs = new HashSet<>();
            int depth = Math.min(outputLength, this.qryScore.get(qid).size());
            double vote = depth * (1.0 / intentNums);
            double slot[] = new double[intentNums + 1];
            while (r.size() < outputLength) {
                Map<Integer, Double> quotient = new HashMap<>();
                int intentSelected = -1;
                double quotientMax = -1.0;
                for (int i = 1; i <= intentNums; i++) {
                    double slotScore = vote / (2 * slot[i] + 1);
                    quotient.put(i, slotScore);
                    if (slotScore > quotientMax) {
                        quotientMax = slotScore;
                        intentSelected = i;
                    }
                }
                int selected = -1;
                double max = Double.MIN_VALUE;
                for (Map.Entry<Integer, List<IdScore>> entries : this.qryScore.get(qid).entrySet()) {
                    int docid = entries.getKey();
                    if (!docs.contains(docid)) {
                        double score = 0.0;
                        List<IdScore> intentScore = entries.getValue();
                        for (int i = 1; i <= intentNums; i++) {
                            for (IdScore docScore : intentScore) {
                                if (docScore.id == i) {
                                    if (i == intentSelected) {
                                        score += lambda * docScore.score * quotient.get(i);
                                    } else {
                                        score += (1.0 - lambda) * docScore.score * quotient.get(i);
                                    }
                                }
                            }
                        }
                        if (score > max) {
                            max = score;
                            selected = docid;
                        }

                    }
                }
                docs.add(selected);
                r.add(selected, max);
                for (int i = 1; i < slot.length; i++) {
                    for (IdScore tmp : this.qryScore.get(qid).get(selected)) {
                        if (tmp.id == i) {
                            slot[i] = slot[i] + tmp.score / max;
                        }
                    }
                }
            }
            r.sort();
            String qryName = String.valueOf(qid);
            File outputFile = new File(this.parameters.get("trecEvalOutputPath"));
            if (r != null) {
                QryEval.printResults(qryName, outputLength, outputFile, r);
            }
        }
    }

    private void runXquad() throws IOException {
        double lambda = Double.parseDouble(this.parameters.get("diversity:lambda"));
        int outputLength = Integer.valueOf(this.parameters.get("diversity:maxResultRankingLength"));
        List<Integer> sortedQid = new ArrayList<>(this.qryScore.keySet());
        Collections.sort(sortedQid);

        for (int qid : sortedQid) {
            ScoreList r = new ScoreList();
            int intentNums = this.qidIntent.get(qid);
            double weight = 1.0 / intentNums;
            double[] penalty = new double[intentNums + 1];
            Arrays.fill(penalty, 1);
            Set<Integer> docs = new HashSet<>();
            while (r.size() < outputLength) {
                int selected = -1;
                double max = Double.MIN_VALUE;
                for (Map.Entry<Integer, List<IdScore>> entries : this.qryScore.get(qid).entrySet()) {
                    int docid = entries.getKey();
                    if (!docs.contains(docid)) {
                        double score = 0.0;
                        List<IdScore> intentScore = entries.getValue();
                        for (int i = 0; i <= intentNums; i++) {
                            for (IdScore docScore : intentScore) {
                                if (docScore.id == i) {
                                    if (i == 0) {
                                        score += (1 - lambda) * docScore.score;
                                    } else {
                                        score += lambda * weight * docScore.score * penalty[i];
                                    }
                                }
                            }
                        }
                        if (score > max) {
                            max = score;
                            selected = docid;
                        }
                    }
                }
                docs.add(selected);
                r.add(selected, max);
                for (int j = 1; j < penalty.length; j++) {
                    for (IdScore ids : this.qryScore.get(qid).get(selected)) {
                        if (ids.id == j) {
                            penalty[j] = penalty[j] * (1.0 - ids.score);
                        }
                    }
                }
            }
            r.sort();
            String qryName = String.valueOf(qid);
            File outputFile = new File(this.parameters.get("trecEvalOutputPath"));
            if (r != null) {
                QryEval.printResults(qryName, outputLength, outputFile, r);
            }
        }
    }
}

class IntentContent<V> {

    public int intendID;
    public ArrayList<V> content = new ArrayList<V>();

    public IntentContent(int intendID) {
        this.intendID = intendID;
    }

}

class IdScore {
    public int id;
    public double score;

    public IdScore(int id, double score) {
        this.id = id;
        this.score = score;
    }
}