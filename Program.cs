using Azure;
using Azure.Messaging;
using Azure.Messaging.EventGrid.Namespaces;

var namespaceEndpoint = "https://kabalu-eg-ns1.westus2-1.eventgrid.azure.net"; 

var topicName = "kabalu-topic1";

var topicKey = "actual key";

var subscriptionName = "queue1";

const short MaxEventCount = 3;

var client = new EventGridReceiverClient(new Uri(namespaceEndpoint), topicName, subscriptionName, new AzureKeyCredential(topicKey));

while (true)
{
    ReceiveResult result = await client.ReceiveAsync(MaxEventCount);

    Console.WriteLine("Received Response");
    Console.WriteLine("-----------------");

    var toRelease = new List<string>();
    var toAcknowledge = new List<string>();
    var toReject = new List<string>();

    foreach (ReceiveDetails detail in result.Details)
    {
        CloudEvent @event = detail.Event;
        BrokerProperties brokerProperties = detail.BrokerProperties;
        Console.WriteLine(@event?.Data?.ToString());

        Console.WriteLine(brokerProperties.LockToken);
        Console.WriteLine();

        if (@event?.Source == "EventPublisherSource" && @event?.Data?.ToObjectFromJson<EventSub>().EventName == "captureevent11")
        {
            toRelease.Add(brokerProperties.LockToken);
        }
        else if (@event?.Source == "EventPublisherSource")
        {
            toAcknowledge.Add(brokerProperties.LockToken);
        }
        else
        {
            toReject.Add(brokerProperties.LockToken);
        }
    }

    if (toRelease.Count > 0)
    {
        ReleaseResult releaseResult = await client.ReleaseAsync(toRelease);

        Console.WriteLine($"Failed count for Release: {releaseResult.FailedLockTokens.Count}");
        foreach (FailedLockToken failedLockToken in releaseResult.FailedLockTokens)
        {
            Console.WriteLine($"Lock Token: {failedLockToken.LockToken}");
            Console.WriteLine($"Error Code: {failedLockToken.Error}");
            Console.WriteLine($"Error Description: {failedLockToken.ToString}");
        }

        Console.WriteLine($"Success count for Release: {releaseResult.SucceededLockTokens.Count}");
        foreach (string lockToken in releaseResult.SucceededLockTokens)
        {
            Console.WriteLine($"Lock Token: {lockToken}");
        }
        Console.WriteLine();
    }

    if (toAcknowledge.Count > 0)
    {
        AcknowledgeResult acknowledgeResult = await client.AcknowledgeAsync(toAcknowledge);

        Console.WriteLine($"Failed count for Acknowledge: {acknowledgeResult.FailedLockTokens.Count}");
        foreach (FailedLockToken failedLockToken in acknowledgeResult.FailedLockTokens)
        {
            Console.WriteLine($"Lock Token: {failedLockToken.LockToken}");
            Console.WriteLine($"Error Code: {failedLockToken.Error}");
            Console.WriteLine($"Error Description: {failedLockToken.ToString}");
        }

        Console.WriteLine($"Success count for Acknowledge: {acknowledgeResult.SucceededLockTokens.Count}");
        foreach (string lockToken in acknowledgeResult.SucceededLockTokens)
        {
            Console.WriteLine($"Lock Token: {lockToken}");
        }
        Console.WriteLine();
    }

    if (toReject.Count > 0)
    {
        RejectResult rejectResult = await client.RejectAsync(toReject);

        Console.WriteLine($"Failed count for Reject: {rejectResult.FailedLockTokens.Count}");
        foreach (FailedLockToken failedLockToken in rejectResult.FailedLockTokens)
        {
            Console.WriteLine($"Lock Token: {failedLockToken.LockToken}");
            Console.WriteLine($"Error Code: {failedLockToken.Error}");
            Console.WriteLine($"Error Description: {failedLockToken.ToString}");
        }

        Console.WriteLine($"Success count for Reject: {rejectResult.SucceededLockTokens.Count}");
        foreach (string lockToken in rejectResult.SucceededLockTokens)
        {
            Console.WriteLine($"Lock Token: {lockToken}");
        }
        Console.WriteLine();
    }
}


public class EventSub
{
    public Guid EventId { get; set; }

    public string EventName { get; set; } = string.Empty;

    public string EventDescription { get; set; } = string.Empty;
}