using eh_schema_test;

await SchemaTester.SendEventWithGeneratedObject();

await SchemaTester.SendEventWithGenericRecord();

await SchemaTester.SendEventThatFailsValidation();