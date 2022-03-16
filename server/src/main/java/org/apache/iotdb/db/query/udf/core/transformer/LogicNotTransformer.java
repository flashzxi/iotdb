package org.apache.iotdb.db.query.udf.core.transformer;

import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.query.udf.core.reader.LayerPointReader;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

import java.io.IOException;

public class LogicNotTransformer extends Transformer {
  private final LayerPointReader layerPointReader;

  public LogicNotTransformer(LayerPointReader layerPointReader) {
    this.layerPointReader = layerPointReader;
  }

  @Override
  public boolean isConstantPointReader() {
    return layerPointReader.isConstantPointReader();
  }

  @Override
  protected boolean cacheValue() throws QueryProcessException, IOException {
    if (!layerPointReader.next()) {
      return false;
    }
    cachedTime = layerPointReader.currentTime();
    if (layerPointReader.isCurrentNull()) {
      currentNull = true;
    } else {
      switch (layerPointReader.getDataType()) {
        case INT32:
          cachedBoolean = layerPointReader.currentInt() == 0;
          break;
        case INT64:
          cachedBoolean = layerPointReader.currentLong() == 0L;
          break;
        case FLOAT:
          cachedBoolean = layerPointReader.currentFloat() == 0.0f;
          break;
        case DOUBLE:
          cachedBoolean = layerPointReader.currentDouble() == 0.0;
          break;
        case BOOLEAN:
          cachedBoolean = !layerPointReader.currentBoolean();
          break;
        default:
          throw new QueryProcessException(
              "Unsupported data type: " + layerPointReader.getDataType().toString());
      }
    }
    layerPointReader.readyForNext();
    return true;
  }

  @Override
  public TSDataType getDataType() {
    return TSDataType.BOOLEAN;
  }
}
