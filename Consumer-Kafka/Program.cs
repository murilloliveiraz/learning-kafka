using Consumer_Kafka;

Console.WriteLine("Iniciando serviços de consumidor Kafka...");

var fraudDetectorService = new FraudDetectorService();
var emailService = new EmailService();
var logService = new LogService();

var cts = new CancellationTokenSource();

Console.CancelKeyPress += (_, e) =>
{
    e.Cancel = true;
    cts.Cancel();   
    Console.WriteLine("\nSinal de cancelamento recebido. Aguardando serviços finalizarem...");
};

var fraudTask = Task.Run(() => fraudDetectorService.ConsumeNewOrders(cts.Token));
var emailTask = Task.Run(() => emailService.ConsumeNewOrders(cts.Token));
var logTask = Task.Run(() => logService.ConsumeNewOrders(cts.Token));

await Task.WhenAll(fraudTask, emailTask);

Console.WriteLine("Todos os serviços de consumidor foram finalizados.");
