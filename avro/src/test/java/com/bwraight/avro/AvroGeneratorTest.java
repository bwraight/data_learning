package com.bwraight.avro;

import com.bwraight.avro.model.Operation;
import com.bwraight.avro.util.AvroGenerator;
import com.bwraight.avro.util.AvroSerializer;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.List;

class AvroGeneratorTest {
   @Test
   void test() throws Exception {
      AvroGenerator avroGenerator = new AvroGenerator();
      AvroSerializer avroSerializer = new AvroSerializer();
      avroGenerator.generateAvroClasses();
      List<Operation> operations = List.of(
            Operation.newBuilder()
                  .setId(1)
                  .build(),
            Operation.newBuilder()
                  .setId(2)
                  .build()
      );

      byte[] jsonBytes = avroSerializer.serializeOperationJSON(operations);
      String json = new String(jsonBytes, StandardCharsets.UTF_8);

      byte[] bytes = avroSerializer.serializeOperationParquet(operations);
      String output = new String(bytes, StandardCharsets.UTF_8);
   }
}