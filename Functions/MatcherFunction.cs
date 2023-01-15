using System;
using System.Threading.Tasks;
using MatcherService.Entities;
using MatcherService.Events;
using Microsoft.Azure.WebJobs;
using Microsoft.Extensions.Logging;
using MongoDB.Bson;
using MongoDB.Driver;
using MongoDB.Driver.Linq;

namespace MatcherService.Functions
{
    public class MatcherFunction
    {
        private const string _databaseName = "CLC-Project";
        private const string _destinationCollectionName = "destinations";
        private const string _matchingsCollectionName = "matchings";
        private const string _dbConnection = "MongoDBAtlasConnection";
        private const string _serviceBusConnection = "ServiceBusConnection";
        private const string _queueName = "destination_crud_events";

        #region Util

        private static IMongoCollection<T> GetCollection<T>(string collectionName)
        {
            var settings = MongoClientSettings.FromConnectionString(Environment.GetEnvironmentVariable(_dbConnection));
            settings.ServerApi = new ServerApi(ServerApiVersion.V1);
            var client = new MongoClient(settings);
            var database = client.GetDatabase(_databaseName);
            var collection = database.GetCollection<T>(collectionName);
            return collection;
        }

        #endregion

        #region Eventhandler

        private static async Task HandleInsertEvent(ObjectId id)
        {
            var destinationsCollection = GetCollection<Destination>(_destinationCollectionName);
            var entry = await destinationsCollection.Find(x => x.Id == id).FirstOrDefaultAsync();

            if (entry is null) return;

            var entries = await destinationsCollection.FindAsync(
                // different user
                x => x.User != entry.User && 
                (
                    // only country matches
                    (x.Country == entry.Country && x.Region     == string.Empty && x.City     == string.Empty) ||
                    (x.Country == entry.Country && entry.Region == string.Empty && entry.City == string.Empty) ||
                    // only country and region matches
                    (x.Country == entry.Country && x.Region == entry.Region && x.City     == string.Empty) ||
                    (x.Country == entry.Country && x.Region == entry.Region && entry.City == string.Empty) ||
                    // country region and city matches
                    (x.Country == entry.Country && x.Region == entry.Region && x.City == entry.City)
                )
            );

            var matching = new Matching { Destination = entry, MatchedDestinations = await entries.ToListAsync() };
            var matchingsCollection = GetCollection<Matching>(_matchingsCollectionName);

            // insert matchings for new destination
            await matchingsCollection.InsertOneAsync(matching);

            // update matchings of existings destinations
            foreach(var dest in matching.MatchedDestinations) { 
                var update = Builders<Matching>.Update.Push(x => x.MatchedDestinations, entry);
                await matchingsCollection.UpdateOneAsync(x => x.Destination.Id == dest.Id, update);
            }
        }

        #endregion

        private static async Task HandleDeleteEvent(ObjectId id)
        {

            var matchingsCollection = GetCollection<Matching>(_matchingsCollectionName);
            var matching = await matchingsCollection.Find(x => x.Destination.Id == id).FirstOrDefaultAsync();

            if (matching is null) return;

            // remove references to deleted destinations in other matchings 
            foreach (var dest in matching.MatchedDestinations)
            {
                var update = Builders<Matching>.Update.Pull(x => x.MatchedDestinations, matching.Destination);
                await matchingsCollection.UpdateOneAsync(x => x.Destination.Id == dest.Id, update);
            }

            // delete matching
            await matchingsCollection.DeleteOneAsync(x => x.Destination.Id == id);
        }

        [FunctionName("Matcher_EventHandler")]
        public static async Task HandleEvent([ServiceBusTrigger(_queueName, Connection = _serviceBusConnection)] DestinationCrudEvent crudEvent,
                                              int deliveryCount,
                                              DateTime enqueuedTimeUtc,
                                              string messageId,
                                              ILogger log)
        {
            log.LogInformation($"{DateTime.Now}: process event of {crudEvent.DestinationId}");
            var id = ObjectId.Parse(crudEvent.DestinationId);
            switch (crudEvent.Type)
            {
                case DestinationCrudEvent.EventType.Insert: await HandleInsertEvent(id); break;
                case DestinationCrudEvent.EventType.Delete: await HandleDeleteEvent(id); break;
            }

        }
    }
}
