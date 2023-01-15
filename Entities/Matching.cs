using MongoDB.Bson.Serialization.Attributes;
using MongoDB.Bson;
using System.Collections.Generic;

namespace MatcherService.Entities
{
    public class Matching
    {
        [BsonId]
        public ObjectId Id { get; set; }

        [BsonElement("Destination")]
        public Destination Destination { get; set; }

        [BsonElement("MatchedDestinations")]
        public IList<Destination> MatchedDestinations { get; set; }
    }
}
