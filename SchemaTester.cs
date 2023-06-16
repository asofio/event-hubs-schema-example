using Avro;
using Avro.Generic;
using Azure.Data.SchemaRegistry;
using Azure.Identity;
using Azure.Messaging.EventHubs;
using Azure.Messaging.EventHubs.Producer;
using Microsoft.Azure.Data.SchemaRegistry.ApacheAvro;
using Microsoft.Azure.Data.SchemaRegistry.example;
using Microsoft.Extensions.Configuration;

namespace eh_schema_test
{
    public class SchemaTester
    {
        private static EventHubProducerClient _producerClient;
        private static SchemaRegistryAvroSerializer _serializer;
        private static SchemaRegistryClient _schemaRegistryClient;
        private static IConfiguration _config;
        private static Settings _settings;

        static SchemaTester()
        {
            _config = new ConfigurationBuilder()
                .AddJsonFile("appsettings.json")
                .AddEnvironmentVariables()
                .Build();
            
            _settings = _config.GetRequiredSection("Settings").Get<Settings>()!;

            _producerClient = new EventHubProducerClient(_settings.EventHubsConnectionString, _settings.EventHubName);
            _schemaRegistryClient = new SchemaRegistryClient(_settings.SchemaRegistryEndpoint, new DefaultAzureCredential());
            _serializer = new SchemaRegistryAvroSerializer(_schemaRegistryClient, _settings.SchemaGroup, new SchemaRegistryAvroSerializerOptions { AutoRegisterSchemas = false });
        }        

        public async static Task SendEventWithGeneratedObject() {

            // Create an Order object using the class generated from the Order.avsc avro schema.
            var sampleOrder = new Order { id = "1234", amount = 45.29, description = "First sample order." };

            // Serialize the Order to EventData.
            EventData eventData = (EventData)await _serializer.SerializeAsync(sampleOrder, messageType: typeof(EventData));

            // Send the event.
            using EventDataBatch eventBatch = await _producerClient.CreateBatchAsync();
            eventBatch.TryAdd(eventData);
            await _producerClient.SendAsync(eventBatch);

            Console.WriteLine("A batch of 1 order has been published using an Order object generated from a local schema.");
        }

        public async static Task SendEventWithGenericRecord() {

            // Get the Order schema from the Event Hubs Schema Registry.
            var schemaRegistryClient = new SchemaRegistryClient(_settings.SchemaRegistryEndpoint, new DefaultAzureCredential());
            var response = schemaRegistryClient.GetSchema(_settings.SchemaIdToTargetInGroup);

            // Create a GenericRecord using the schema and populate fields within the schema.
            var schema = (RecordSchema)Avro.Schema.Parse(response.Value.Definition);
            GenericRecord genericRecord = new GenericRecord(schema);
            genericRecord.Add("id", "my-new-id");
            genericRecord.Add("amount", 100.50);
            genericRecord.Add("description", "my-new-description");

            // Serialize the GenericRecord to EventData.
            EventData eventData = (EventData)await _serializer.SerializeAsync(genericRecord, messageType: typeof(EventData));

            // Send the event.
            using EventDataBatch eventBatch = await _producerClient.CreateBatchAsync();
            eventBatch.TryAdd(eventData);
            await _producerClient.SendAsync(eventBatch);

            Console.WriteLine("A batch of 1 order has been published using a schema pulled from the Event Hubs Schema Registry.");            
        }

        public async static Task SendEventThatFailsValidation() {

            // Create an Order object using the class generated from the Order.avsc avro schema.
            var sampleOrder = new BadOrder { foo = "bar"};

            try {
                // Serialize the Order to EventData. This will fail because the schema "BadOrder" does not exist within the Event Hubs Schema Registry.
                EventData eventData = (EventData)await _serializer.SerializeAsync(sampleOrder, messageType: typeof(EventData));

                // Attempt to send the event.
                using EventDataBatch eventBatch = await _producerClient.CreateBatchAsync();
                eventBatch.TryAdd(eventData);
                await _producerClient.SendAsync(eventBatch);
            }
            catch (Exception e) {
                Console.WriteLine($"An exception was thrown when sending the event due to schema validation failing: {e.Message}");
            }
        }
    }
}