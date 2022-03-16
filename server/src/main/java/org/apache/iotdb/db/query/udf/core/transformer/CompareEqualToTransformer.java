package org.apache.iotdb.db.query.udf.core.transformer;

import org.apache.iotdb.db.query.udf.core.reader.LayerPointReader;

public class CompareEqualToTransformer extends CompareOperatorTransformer {

  public CompareEqualToTransformer(
      LayerPointReader leftPointReader, LayerPointReader rightPointReader) {
    super(leftPointReader, rightPointReader);
  }

  @Override
  protected double evaluate(double leftOperand, double rightOperand) {
    return leftOperand == rightOperand ? 1.0 : 0.0;
  }
}
