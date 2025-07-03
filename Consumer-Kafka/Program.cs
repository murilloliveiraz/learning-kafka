using Consumer_Kafka;

Console.WriteLine("Iniciando serviços de consumidor Kafka...");

var fraudDetectorService1 = new FraudDetectorService();
var fraudDetectorService2 = new FraudDetectorService();
var emailService = new EmailService();
var logService = new LogService();

var cts = new CancellationTokenSource();

Console.CancelKeyPress += (_, e) =>
{
    e.Cancel = true;
    cts.Cancel();   
    Console.WriteLine("\nSinal de cancelamento recebido. Aguardando serviços finalizarem...");
};

var fraudTask1 = Task.Run(() => fraudDetectorService1.Start(cts.Token));
var fraudTask2 = Task.Run(() => fraudDetectorService2.Start(cts.Token));
var emailTask = Task.Run(() => emailService.Start(cts.Token));
var logTask = Task.Run(() => logService.Start(cts.Token));

await Task.WhenAll(fraudTask1, fraudTask2, emailTask);

Console.WriteLine("Todos os serviços de consumidor foram finalizados.");
