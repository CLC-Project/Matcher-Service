using MongoDB.Bson.Serialization.Attributes;
using MongoDB.Bson;

namespace MatcherService.Entities
{
    public class Destination
    {
        [BsonId]
        public ObjectId Id { get; set; }

        [BsonElement("Country")]
        public string Country { get; set; }

        [BsonElement("Region")]
        public string Region { get; set; }

        [BsonElement("City")]
        public string City { get; set; }

        [BsonElement("User")]
        public string User { get; set; }

    }
}
