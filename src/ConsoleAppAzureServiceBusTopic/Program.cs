using Azure.Messaging.ServiceBus;
using Bogus.DataSets;
using ConsoleAppAzureServiceBusTopic.Utils;
using Serilog;
using Testcontainers.ServiceBus;

var logger = new LoggerConfiguration()
    .WriteTo.Console()
    .WriteTo.File("testcontainers-azureservicebus-topic.tmp")
    .CreateLogger();
logger.Information("***** Iniciando testes com Testcontainers + Azure Service Bus Topic *****");

var configFile = Path.Combine(Directory.GetCurrentDirectory(), "Config.json");
logger.Information($"Arquivo de configuracoes do Azure Service Bus: {configFile}");

CommandLineHelper.Execute("docker container ls",
    "Containers antes da execucao do Testcontainers...");

var serviceBusContainer = new ServiceBusBuilder()
  .WithImage("mcr.microsoft.com/azure-messaging/servicebus-emulator:1.1.2")
  .WithBindMount(configFile, "/ServiceBus_Emulator/ConfigFiles/Config.json")
  .WithAcceptLicenseAgreement(true)
  .Build();
await serviceBusContainer.StartAsync();

CommandLineHelper.Execute("docker container ls",
    "Containers apos execucao do Testcontainers...");

var connectionAzureServiceBus = serviceBusContainer.GetConnectionString();
const string topic = "topic-teste";
logger.Information($"Connection String = {connectionAzureServiceBus}");
logger.Information($"Topico a ser utilizado nos testes = {topic}");

var client = new ServiceBusClient(connectionAzureServiceBus);
var sender = client.CreateSender(topic);
const int maxMessages = 10;
var lorem = new Lorem("pt_BR");
for (int i = 1; i <= maxMessages; i++)
{
    var sentence = lorem.Sentence();
    logger.Information($"Enviando mensagem {i}/{maxMessages}: {sentence}");
    await sender.SendMessageAsync(new ServiceBusMessage(sentence));
}
logger.Information("Pressione ENTER para continuar...");
Console.ReadLine();

await ConsumeMessageFromSubscriptionAsync("subscription01");
logger.Information("Pressione ENTER para continuar...");
Console.ReadLine();

await ConsumeMessageFromSubscriptionAsync("subscription02");
logger.Information("Pressione ENTER para continuar...");
Console.ReadLine();

Console.WriteLine("Testes concluidos com sucesso!");


async Task ConsumeMessageFromSubscriptionAsync(string subscriptionName)
{
    logger.Information($"Processamento para a subscription: {subscriptionName}");

    var client1 = new ServiceBusClient(connectionAzureServiceBus);
    var opt1 = new ServiceBusProcessorOptions()
    {
        ReceiveMode = ServiceBusReceiveMode.PeekLock
    };
    var processor1 = client1.CreateProcessor(topic, subscriptionName, opt1);

    processor1.ProcessMessageAsync += MessageHandler;
    processor1.ProcessErrorAsync += ErrorHandler;

    logger.Information($"Iniciando o processamento e aguardando 10 segundos...");
    await processor1.StartProcessingAsync();

    await Task.Delay(TimeSpan.FromSeconds(10));

    await processor1.StopProcessingAsync();
    await processor1.DisposeAsync();
    await client1.DisposeAsync();
    logger.Information($"Encerrado o processamento para a subscription: {subscriptionName}");
}

async Task MessageHandler(ProcessMessageEventArgs args)
{
    logger.Information($"Mensagem recebida: SequenceNumber = {args.Message.SequenceNumber} | " +
        $"Body = {args.Message.Body.ToString()}");
    await args.CompleteMessageAsync(args.Message);
}

Task ErrorHandler(ProcessErrorEventArgs args)
{
    logger.Error($"Erro durante o processamento das mensagens {args.Exception.Message} | " +
        $"{args.Exception.GetType().FullName}");
    return Task.CompletedTask;
}