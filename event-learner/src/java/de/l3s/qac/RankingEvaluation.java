package de.l3s.qac;

import ciir.umass.edu.learning.DataPoint;
import ciir.umass.edu.learning.RankList;
import ciir.umass.edu.metric.ERRScorer;
import ciir.umass.edu.metric.NDCGScorer;
import com.google.common.base.Charsets;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.io.Files;
import org.joda.time.LocalDate;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import java.io.*;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class RankingEvaluation {

    DateTimeFormatter dateFormat = DateTimeFormat.forPattern("yyyyMMdd");


    public static void main(String[] args) throws IOException {
        RankingEvaluation e = new RankingEvaluation();


        File inputFile = new File(args[0]);
        if (inputFile.exists() && inputFile.isDirectory()) {

            File[] files = inputFile.listFiles();
            BufferedWriter bw = Files.newWriter(new File("/home/tunguyen/qac/results/" + args[1] + ".txt"), Charsets.UTF_8);

            for (File f : files) {


                List<String> c = Files.readLines(f, Charsets.UTF_8);
                //annotation file


                Map<String, Pair> map = Maps.newHashMap();
                e.loadAnnotation("/home/tunguyen/qac/label/aol_knn_implicit_anticipated_queries.label", map);
                e.loadAnnotation("/home/tunguyen/qac/label/aol_knn_implicit_ongoing_queries.label", map);
                e.loadAnnotation("/home/tunguyen/qac/label/aol_knn_implicit_breaking_queries.label", map);

                e.loadAnnotation("/home/tunguyen/qac/label/aol_knn_explicit_anticipated_queries.label", map);
                e.loadAnnotation("/home/tunguyen/qac/label/aol_knn_explicit_ongoing_queries.label", map);
                e.loadAnnotation("/home/tunguyen/qac/label/aol_knn_explicit_breaking_queries.label", map);


                List<String> ls = Lists.newArrayList();
                String query;

                double err1_total = 0;
                double err3_total = 0;
                double err5_total = 0;
                double err10_total = 0;

                double ndcg1_total = 0;
                double ndcg3_total = 0;
                double ndcg5_total = 0;
                double ndcg10_total = 0;


                int i = 0;
                for (String line : c) {
                    if (line.contains("########")) {
                        i++;
//                		if (line.contains("rwrw")) 
//                			query = line.replace("######## parInfo: rwrw ranking ", "");
//                		else 
                        query = line.replace("######## ", "");
                        System.out.println(query);
                        RankList rLs = e._constructRankList(map, ls, query, f.getName());
                        ERRScorer err1 = new ERRScorer(1);
                        ERRScorer err3 = new ERRScorer(3);
                        ERRScorer err5 = new ERRScorer(5);
                        ERRScorer err10 = new ERRScorer(10);

                        err1_total += err1.score(rLs);
                        err3_total += err3.score(rLs);
                        err5_total += err5.score(rLs);
                        err10_total += err10.score(rLs);

                        System.out.println("ERR@10 " + i + ": " + err10_total);
                        NDCGScorer ndcg1 = new NDCGScorer(10);
                        NDCGScorer ndcg3 = new NDCGScorer(10);
                        NDCGScorer ndcg5 = new NDCGScorer(10);
                        NDCGScorer ndcg10 = new NDCGScorer(10);

                        ndcg1_total += ndcg1.score(rLs);
                        ndcg3_total += ndcg3.score(rLs);
                        ndcg5_total += ndcg5.score(rLs);
                        ndcg10_total += ndcg10.score(rLs);

                        System.out.println("NDCG@10: " + i + ": " + ndcg10_total);

                        //clear list

                        ls.clear();

                    } else {
                        String l = line.split("\t")[0];
                        ls.add(l);
                    }


                }

                bw.append(f.getName() + "\t" + (double) err1_total / i + "\t" + (double) err3_total / i + "\t" + (double) err5_total / i + "\t" + (double) err10_total / i
                        + "\t" + (double) ndcg1_total / i + "\t" + (double) ndcg3_total / i + "\t" + (double) ndcg5_total / i + "\t" + (double) ndcg10_total / i);

                bw.newLine();


            }

            bw.flush();
            bw.close();


        }
    }

    public static void _main(String[] args) throws IOException {
        RankingEvaluation e = new RankingEvaluation();


        File inputFile = new File(args[0]);
        if (inputFile.exists() && inputFile.isDirectory()) {

            File[] files = inputFile.listFiles();
            BufferedWriter bw = Files.newWriter(new File("/home/tunguyen/qac/results/implicit.txt"), Charsets.UTF_8);

            for (File f : files) {
                String _path = f.getName();
                if (!_path.startsWith("q-knn_")) continue;
                String dates = _path.contains("RMLE") ? _path.substring(_path.length() - "2006-02-28_2006-03-31_aol_RMLE.txt".length(), _path.length())
                        : _path.substring(_path.length() - "2006-02-28_2006-03-31_aol_MLE.txt".length(), _path.length());

                System.out.println("Dates: " + dates);

                _path = _path.replace("rwrw_ranking_", "");
                _path = _path.replace("q-sim_", "");
                _path = _path.replace("q-knn_", "");
                String _query = null;
                if (_path.contains("-fix")) {
                    _query = _path.substring(0, _path.indexOf("-fix"));
                } else if (_path.contains("-vary")) {
                    _query = _path.substring(0, _path.indexOf("-vary"));
                } else if (_path.contains("-burst")) {
                    _query = _path.substring(0, _path.indexOf("-burst"));
                }
                String query = _query.replaceAll("_", " ");
                dates = dates.replaceAll("_aol.dat", "");
                String[] _dates = dates.split("\\_");


                List<String> c = Files.readLines(f, Charsets.UTF_8);
                //annotation file


                Map<String, Pair> map = Maps.newHashMap();
                e.loadAnnotation("/home/tunguyen/qac/label/aol_knn_implicit_anticipated_queries.label", map);

                e.loadAnnotation("/home/tunguyen/qac/label/aol_knn_implicit_ongoing_queries.label", map);
                e.loadAnnotation("/home/tunguyen/qac/label/aol_knn_implicit_breaking_queries.label", map);


                RankList rLs = e.constructRankList(map, c, query, _dates[1]);
                ERRScorer err1 = new ERRScorer(1);
                ERRScorer err3 = new ERRScorer(3);
                ERRScorer err5 = new ERRScorer(5);
                ERRScorer err10 = new ERRScorer(10);

                System.out.println("ERR@10: " + err10.score(rLs));
                NDCGScorer ndcg1 = new NDCGScorer(10);
                NDCGScorer ndcg3 = new NDCGScorer(10);
                NDCGScorer ndcg5 = new NDCGScorer(10);
                NDCGScorer ndcg10 = new NDCGScorer(10);

                System.out.println("NDCG@10: " + ndcg10.score(rLs));

                bw.append(f.getName() + "\t" + err1.score(rLs) + "\t" + err3.score(rLs) + "\t" + err5.score(rLs) + "\t" + err10.score(rLs)
                        + "\t" + ndcg1.score(rLs) + "\t" + ndcg3.score(rLs) + "\t" + ndcg5.score(rLs) + "\t" + ndcg10.score(rLs));

                bw.newLine();
            }

            bw.flush();
            bw.close();


        }
    }


    public RankList constructRankList(Map<String, Pair> map, List<String> c, String query, String hTime) {
        RankList rLs = new RankList();
        hTime = hTime.replaceAll("-", "");
        LocalDate _hTime = LocalDate.parse(hTime, dateFormat);
        Set<String> keys = map.keySet();

        String eTime = map.get(keys.iterator().next()).date;
        int i;
        if (eTime.contains("-")) {
            LocalDate sTime = LocalDate.parse(eTime.split("\\-")[0].trim(), dateFormat);
            LocalDate endTime = LocalDate.parse(eTime.split("\\-")[1].trim(), dateFormat);

            if (_hTime.isBefore(sTime)) i = 0;
            else if (_hTime.isAfter(endTime)) i = 2;
            else i = 1;
        } else {
            LocalDate _eTime = LocalDate.parse(eTime, dateFormat);
            if (_hTime.isBefore(_eTime)) i = 0;
            else if (_hTime.isAfter(_eTime)) i = 2;
            else i = 1;
        }

        for (String candidate : c) {
            Pair p = map.get(query.trim() + "\t" + candidate.trim());
            if (p == null) continue;
            DataPoint dp = new DataPoint(candidate, p.label.values[i]);

            rLs.add(dp);
        }
        return rLs;


    }

    public RankList _constructRankList(Map<String, Pair> map, List<String> c, String query, String fName) {
        RankList rLs = new RankList();

        String hTime = fName.substring(fName.indexOf("_2006") + 1, fName.indexOf("_aol"));
        int i;

        if (fName.contains("after")) i = 2;
        else if (fName.contains("during")) i = 1;
        else if (fName.contains("before")) i = 0;

        else return constructRankList(map, c, query, hTime);

        for (String candidate : c) {
            query = query.trim();
            candidate = candidate.trim();
            Pair p = map.get(query + "\t" + candidate);
            if (p == null) {
                continue;
            }
            DataPoint dp = new DataPoint(candidate, p.label.values[i]);

            rLs.add(dp);
        }
        return rLs;


    }

    public void loadAnnotation(String path, Map<String, Pair> map) {
        try {
            BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(path)));
            String line;
            while ((line = br.readLine()) != null) {
                if (line.trim().equals("")) continue;
                String[] token = line.split("\t");
                String[] c = token[0].split("\\[");
                String query = c[0].trim();
                String eventDate = c[1].replace("]", "").trim();
                String candidate = token[1].trim();

                Label label = new Label();
                label.values[0] = getLabel(token[2]);
                label.values[1] = getLabel(token[3]);
                label.values[2] = getLabel(token[4]);
                map.put(query + "\t" + candidate, new Pair(label, eventDate));

            }
        } catch (FileNotFoundException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }


    public int getLabel(String _label) {
        int label;
        if (_label.equals("null")) _label = "" + 0;
        switch (Integer.parseInt(_label.split("\\:")[0])) {
            case -1: //VR
                label = 3;
                break;
            case 0: //R
                label = 2;
                break;
            case 1: //NR
                label = 1;
                break;
            case 2: //NA
                label = 0;
                break;
            default:
                label = 0;
                break;
        }
        return label;
    }

    public class Label {
        public int[] values = new int[3];
    }

    public class Pair {
        public Label label;
        public String date;

        public Pair(Label label, String date) {
            this.label = label;
            this.date = date;
        }
    }

}
