package com.bwraight.avro.util;

import org.apache.avro.Schema;
import org.apache.avro.compiler.specific.SpecificCompiler;

import java.io.File;
import java.io.IOException;

public class AvroGenerator {
   public void generateAvroClasses() throws IOException {
      SpecificCompiler compiler = new SpecificCompiler(new Schema.Parser().parse(new File("src/main/resources/com/bwraight/avro/model/operation-schema.avsc")));
      compiler.compileToDestination(new File("src/main/resources/com/bwraight/avro/model"), new File("src/main/java"));
   }
}
