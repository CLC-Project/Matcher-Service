namespace MatcherService.Events
{
    public class DestinationCrudEvent
    {
        public enum EventType
        {
            Insert,
            Delete
        }

        public EventType Type { get; set; }

        public string DestinationId { get; set; }
    }
}
