namespace eh_schema_test
{
    public class Settings
    {
        public string? EventHubsConnectionString { get; set; }
        public string? EventHubName { get; set; }
        public string? SchemaRegistryEndpoint { get; set; }
        public string? SchemaGroup { get; set; }
        public string? SchemaIdToTargetInGroup { get; set; }
    }
}