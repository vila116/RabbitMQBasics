using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System.Text;
// connection facotry 
var factory = new ConnectionFactory { HostName = "localhost" };
// open an new connection to my RabbitMQ broker
using var connection = await factory.CreateConnectionAsync();
//instantiate a channel - way to communicate with my broker instance
using var channel = await connection.CreateChannelAsync();
///<Summary> 
///declaring queue with the same name as producer "messages"
///</Summary>>

await channel.QueueDeclareAsync(
    queue: "messages",
    durable: true,
    exclusive: false,
    autoDelete: false,
    arguments: null);
Console.WriteLine("waitng for messages....");

var consumer = new AsyncEventingBasicConsumer(channel);
consumer.ReceivedAsync += async (sender, eventArgs) =>
{
    byte[] body = eventArgs.Body.ToArray();
    string message = Encoding.UTF8.GetString(body);

    Console.WriteLine($"Received:{message}");

    // ack to broker tht msg is received so msg can be removed from the queue 
    // so any concurrent consumers wont be able to process this message
    await ((AsyncEventingBasicConsumer)sender).Channel.BasicAckAsync(eventArgs.DeliveryTag,
        multiple:false);
};
await channel.BasicConsumeAsync("messages", autoAck: false, consumer);

Console.ReadLine();