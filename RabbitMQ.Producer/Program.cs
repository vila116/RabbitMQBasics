
using RabbitMQ.Client;
using System.Text;
// connection facotry 
var factory = new ConnectionFactory { HostName ="localhost"};
// open an new connection to my RabbitMQ broker
using var connection = await factory.CreateConnectionAsync();
//instantiate a channel - way to communicate with my broker instance
using var channel = await connection.CreateChannelAsync();
#region
// channel is how we can interact with a broker 
// bind queue  , declare a queue , delete it , unbind etc
// work with exchanges using exchange API's
// there is also support for Acknowledging and consuming messages
#endregion
await channel.QueueDeclareAsync(
    queue: "messages",
    durable: true,
    exclusive: false,
    autoDelete: false,
    arguments:null);
// producer to publish some message
for (int i = 0; i < 10; i++)
{
    var message = $"{DateTime.UtcNow}-{Guid.NewGuid()}";
    var body = Encoding.UTF8.GetBytes(message);
    // publish to the channel instace queue creatd above
    await channel.BasicPublishAsync(
        exchange:string.Empty,
        routingKey:"messages",
        mandatory:true,
        basicProperties: new BasicProperties {Persistent=true },
        body:body);
    Console.WriteLine($"Sent_{i}:{message}");
    //delay to observe
    await Task.Delay(5000);
}

