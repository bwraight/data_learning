package com.bwraight.avro.util;

import com.bwraight.avro.model.Operation;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.List;

@Slf4j
public class AvroSerializer {
   public byte[] serializeOperationJSON(List<Operation> operations) {
      DatumWriter<Operation> writer = new SpecificDatumWriter<>(Operation.class);
      byte[] data = new byte[0];
      ByteArrayOutputStream stream = new ByteArrayOutputStream();
      Encoder jsonEncoder = null;
      try {
         jsonEncoder = EncoderFactory.get().jsonEncoder(Operation.getClassSchema(), stream);
         for (Operation operation : operations) {
            writer.write(operation, jsonEncoder);
         }
         jsonEncoder.flush();
         data = stream.toByteArray();
      } catch (IOException e) {
         LOG.error("Serialization error " + e.getMessage());
      }
      return data;
   }

   public byte[] serializeOperationParquet(List<Operation> operations) throws IOException {
      ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
      ParquetBufferedWriter pbw = new ParquetBufferedWriter(new BufferedOutputStream(byteArrayOutputStream));

      try (ParquetWriter<Object> pw = AvroParquetWriter.builder(pbw)
            .withSchema(Operation.getClassSchema())
            .withCompressionCodec(CompressionCodecName.SNAPPY)
            .withWriterVersion(ParquetProperties.WriterVersion.PARQUET_2_0)
            .enableDictionaryEncoding()
            .build()) {

         for (Operation operation : operations) {
            pw.write(operation);
         }
      }

      return byteArrayOutputStream.toByteArray();
   }

}
