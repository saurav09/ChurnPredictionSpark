package com.demo.spark;

import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.classification.RandomForestClassifier;
import org.apache.spark.ml.feature.*;
import org.apache.spark.sql.DataFrame;

import java.io.IOException;

/**
 * Created by sauraraj on 8/27/2016.
 */
public class ModelGeneration {

    private PipelineModel pipelineModel;
    private Pipeline pipeline;
    private RandomForestClassifier rfc;
    public ModelGeneration(DataFrame data) throws IOException {

        VectorIndexerModel featureIndexer = new VectorIndexer().setInputCol("features").setOutputCol("indexedFeatures").setMaxCategories(4).fit(data);

        RandomForestClassifier rf = new RandomForestClassifier()
                .setLabelCol("Actual")
                .setFeaturesCol("indexedFeatures");

//        IndexToString labelConverter = new IndexToString()
//                .setInputCol("prediction")
//                .setOutputCol("predictedLabel")
//                .setLabels(labelIndexer.labels());

        Pipeline pipelineNew = new Pipeline()
                .setStages(new PipelineStage[] {featureIndexer,rf});

        PipelineModel model = pipelineNew.fit(data);
        pipelineModel = model;
        pipeline = pipelineNew;
        rfc = rf;
    }

    PipelineModel getModel(){
        return pipelineModel;
    }

    Pipeline getPipeline(){
        return pipeline;
    }

    RandomForestClassifier getClassifier(){
        return rfc;
    }
}
