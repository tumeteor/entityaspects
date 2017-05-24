package l3s.de.event.classifier;

import weka.classifiers.AbstractClassifier;
import weka.classifiers.Classifier;
import weka.classifiers.Evaluation;
import weka.classifiers.functions.LibSVM;
import weka.classifiers.meta.FilteredClassifier;
import weka.core.Instances;
import weka.core.Utils;
import weka.filters.Filter;
import weka.filters.supervised.attribute.AddClassification;
import weka.filters.unsupervised.attribute.Normalize;
import weka.filters.unsupervised.attribute.PrincipalComponents;
import weka.filters.unsupervised.attribute.Remove;
import weka.filters.unsupervised.attribute.ReplaceMissingValues;

import java.io.*;
import java.util.Random;

public class EventQueryLogClassifier {

    public static void main(String[] args) {
        //partition by fix-time

        // load training data
        try {
            weka.core.Instances data = new weka.core.Instances(new InputStreamReader(EventQueryLogClassifier.class.getResourceAsStream("/training_data/train/train/expl/aol_expl_3labels_par0307.arff")));
            //Clean up training data
            ReplaceMissingValues replace = new ReplaceMissingValues();
            replace.setInputFormat(data);
            Instances data_filter = Filter.useFilter(data, replace);
            int cIdx = data_filter.numAttributes() - 1;
            data_filter.setClassIndex(cIdx);
            // filter
            Remove rm = new Remove();
            rm.setAttributeIndices("1");  // remove 1st attribute

            //pca
            PrincipalComponents pc = new PrincipalComponents();
            Normalize normalize = new Normalize();
            // build and evaluate classifier
            //J48 classifier = new J48();
            FilteredClassifier fc = new FilteredClassifier();
            // using an unpruned J48
            // classifier.setUnpruned(true);
            // Classifier classifier = new NaiveBayes();
            // Classifier classifier = new SMO();
            // MultilayerPerceptron classifier = new MultilayerPerceptron();
            // meta-classifier
            LibSVM classifier = new LibSVM();
            // perform cross-validation
            int runs = 1;
            int folds = 10;
            for (int i = 0; i < runs; i++) {
                // randomize data
                int seed = i + 1;
                Random rand = new Random(seed);
                Instances randData = new Instances(data_filter);
                randData.randomize(rand);
                if (randData.classAttribute().isNominal())
                    randData.stratify(folds);
                Evaluation eval = new Evaluation(randData);
                // perform cross-validation and add predictions
                Instances predictedData = null;

                for (int n = 0; n < folds; n++) {
                    Instances train = randData.trainCV(folds, n);
                    Instances test = randData.testCV(folds, n);
                    Classifier cls = AbstractClassifier.makeCopy(classifier);
                    // cls.buildClassifier(train);
                    // eval.evaluateModel(cls, test);
                    // the above code is used by the StratifiedRemoveFolds filter, the
                    // code below by the Explorer/Experimenter:
                    // Instances train = randData.trainCV(folds, n, rand);
                    fc.setFilter(rm);
                    fc.setFilter(pc);
                    fc.setFilter(normalize);
                    fc.setClassifier(cls);
                    fc.buildClassifier(train);
                    eval.evaluateModel(fc, test);
                    // output evaluation
                    // add predictions
                    AddClassification filter = new AddClassification();
                    filter.setClassifier(fc);
                    filter.setOutputClassification(true);
                    filter.setOutputDistribution(true);
                    filter.setOutputErrorFlag(true);
                    filter.setInputFormat(train);
                    Filter.useFilter(train, filter);  // trains the classifier
                    Instances pred = Filter.useFilter(test, filter);  // perform predictions on test set
                    if (predictedData == null)
                        predictedData = new Instances(pred, 0);
                    for (int j = 0; j < pred.numInstances(); j++)
                        predictedData.add(pred.instance(j));
                }
                // output evaluation
                System.out.println();
                System.out.println("=== Setup ===");
                System.out.println("Classifier: " + fc.getClass().getName() + " " + Utils.joinOptions(fc.getOptions()));
                System.out.println("Dataset: " + data.relationName());
                System.out.println("Folds: " + folds);
                System.out.println("Seed: " + seed);
                System.out.println();
                System.out.println(eval.toSummaryString("=== " + folds + "-fold Cross-validation ===", false));
                BufferedWriter out0 = new BufferedWriter(new OutputStreamWriter(new FileOutputStream("aol_expl2_4labels_par0521.out")));
                for (int j = 0; j < predictedData.numInstances(); j++) {
                    out0.write(predictedData.instance(j).toString());
                    out0.newLine();
                    out0.flush();
                }
                out0.close();
            }
        } catch (FileNotFoundException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

    }
}
