package com.demo.spark;

import org.apache.spark.ml.Model;
import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.classification.RandomForestClassifier;
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator;
import org.apache.spark.ml.param.ParamMap;
import org.apache.spark.ml.tuning.CrossValidator;
import org.apache.spark.ml.tuning.CrossValidatorModel;
import org.apache.spark.ml.tuning.ParamGridBuilder;
import org.apache.spark.sql.DataFrame;

/**
 * Created by sauraraj on 8/27/2016.
 */
public class ModelImprovement {
    private Model model;
    public ModelImprovement(RandomForestClassifier rf, Pipeline pipelineNew, DataFrame data) {

        ParamMap[] paramGrid = new ParamGridBuilder()
                .addGrid(rf.maxBins(),new int[]{25,50})
                .addGrid(rf.maxDepth(),new int[]{6,10})
                .build();

        BinaryClassificationEvaluator evaluatorNew = new BinaryClassificationEvaluator()
                .setLabelCol("Actual");

        CrossValidatorModel crossValidator = new CrossValidator()
                .setEstimator(pipelineNew)
                .setEvaluator(evaluatorNew)
                .setEstimatorParamMaps(paramGrid)
                .setNumFolds(5)
                .fit(data);

        Model<?> bestModel = crossValidator.bestModel();
        model = bestModel;
    }

    Model getBestModel(){
        return model;
    }
}
