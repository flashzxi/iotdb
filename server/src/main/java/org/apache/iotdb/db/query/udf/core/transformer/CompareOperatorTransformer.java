package org.apache.iotdb.db.query.udf.core.transformer;

import org.apache.iotdb.db.query.udf.core.reader.LayerPointReader;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

public abstract class CompareOperatorTransformer extends ArithmeticBinaryTransformer {

  protected CompareOperatorTransformer(
      LayerPointReader leftPointReader, LayerPointReader rightPointReader) {
    super(leftPointReader, rightPointReader);
  }

  @Override
  public TSDataType getDataType() {
    cachedBoolean = (cachedDouble == 1.0);
    return TSDataType.BOOLEAN;
  }
}
