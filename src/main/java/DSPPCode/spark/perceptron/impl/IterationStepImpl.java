package DSPPCode.spark.perceptron.impl;

import DSPPCode.spark.perceptron.question.DataPoint;
import DSPPCode.spark.perceptron.question.IterationStep;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.broadcast.Broadcast;


public class IterationStepImpl extends IterationStep{
  public Broadcast<double[]> createBroadcastVariable(JavaSparkContext sc,
      double[] localVariable) {
    return sc.broadcast(localVariable);
  }

  public boolean termination(double[] old, double[] newWeightsAndBias) {
    double diff = 0.0;
    for (int i = 0; i < old.length; i++) {
      diff += (old[i] - newWeightsAndBias[i]) * (old[i] - newWeightsAndBias[i]);
    }
    return diff < IterationStep.THRESHOLD;
  }

  public static class VectorSum implements Function2<double[], double[], double[]> {
    public double[] call(double[] a, double[] b) throws Exception {
      assert a.length == b.length;
      double[] result = new double[a.length];
      for (int i = 0; i < a.length; i++) {
        result[i] = a[i] + b[i];
      }
      return result;
    }
  }

  public double[] runStep(JavaRDD<DataPoint> points, Broadcast<double[]> broadcastWeightsAndBias) {
    double[] weightsAndBias = broadcastWeightsAndBias.getValue();
    double[] gradient = points.map(new ComputeGradient(weightsAndBias)).reduce(new VectorSum());
    System.out.println("gradient: " + gradient[0] + " " + gradient[1] + " " + gradient[2]);
    for (int i = 0; i < weightsAndBias.length; i++) {
      weightsAndBias[i] += STEP * gradient[i] / 2; // fuck u
    }
    return weightsAndBias;
  }

  public static class ComputeGradient implements Function<DataPoint, double[]> {
    public final double[] weightsAndBias;

    public ComputeGradient(double[] weightsAndBias) {
      this.weightsAndBias = weightsAndBias;
    }

    @Override
    public double[] call(DataPoint dataPoint) throws Exception {
      // forward
      double sum = 0.0;
      for (int i = 0; i < dataPoint.x.length; i++) {
        sum += dataPoint.x[i] * weightsAndBias[i];
      }
      sum += weightsAndBias[weightsAndBias.length - 1];
      double predict = sum >= 0 ? 1 : -1;

      // double error = dataPoint.y - predict == 0 ? 0 : dataPoint.y;
      double error = dataPoint.y - predict;
      if (error != 0) {
        System.out.println(dataPoint + " " + predict);
        System.out.println("sum: " + sum);
        System.out.println("weights: " + weightsAndBias[0] + " " + weightsAndBias[1] + " " + weightsAndBias[2]);
      }
      double[] grad = new double[weightsAndBias.length];
      for (int i = 0; i < grad.length - 1; i++) {
        grad[i] = error * dataPoint.x[i];
      }
      grad[grad.length - 1] = error;

      return grad;
    }
  }
}

