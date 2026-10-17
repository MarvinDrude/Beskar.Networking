using System.Net;
using Beskar.Jobs.Client;
using Beskar.Jobs.Common.Attributes;
using Beskar.Jobs.Common.Interfaces;
using Beskar.Jobs.Scheduler;
using Beskar.Jobs.Worker;
using Beskar.Mqtt.Server;

Console.WriteLine("========================================================================");
Console.WriteLine("  Beskar.Jobs — Source-Generated Handlers & Chained Continuations Demo");
Console.WriteLine("========================================================================");

var endPoint = new IPEndPoint(IPAddress.Loopback, 18884);

var broker = MqttServerFactory.CreateBuilder()
   .WithDefaultClientIdGenerator()
   .UseTcp(endPoint.Port)
   .Build();
await broker.StartAsync();

await using var scheduler = JobSchedulerBuilder.Create()
   .WithTickInterval(TimeSpan.FromMilliseconds(25))
   .UseTcp(endPoint)
   .Build();

scheduler.Events.OnJobDispatched.Add((ctx, _) =>
{
   Console.WriteLine($"[Scheduler] Dispatched '{ctx.Job.JobType}' ({ctx.Job.JobId}) on queue '{ctx.Job.Queue}' -> Worker '{ctx.WorkerId}'");
   return ValueTask.CompletedTask;
});

await scheduler.StartAsync();

// Spin up two workers with overlapping & specialized queues to show queue + capability routing
await using var workerAlpha = JobWorkerBuilder.Create()
   .WithWorkerId("worker-orders-alpha")
   .WithMaxConcurrency(2)
   .RegisterGeneratedHandlerWithResult<ValidateOrderJobHandler, OrderWorkflowArgs, OrderValidationResult>()
   .RegisterGeneratedHandlerWithResult<RenderInvoicePdfJobHandler, OrderWorkflowArgs, InvoicePdfResult>()
   .UseTcp(endPoint)
   .Build();

await using var workerBeta = JobWorkerBuilder.Create()
   .WithWorkerId("worker-mailer-beta")
   .WithMaxConcurrency(2)
   .RegisterGeneratedHandlerWithResult<RenderInvoicePdfJobHandler, OrderWorkflowArgs, InvoicePdfResult>()
   .RegisterGeneratedHandler<SendReceiptEmailJobHandler, OrderWorkflowArgs>()
   .UseTcp(endPoint)
   .Build();

await workerAlpha.StartAsync();
await workerBeta.StartAsync();

await using var client = JobClientBuilder.Create()
   .UseTcp(endPoint)
   .Build();
await client.ConnectAsync();

var workflowArgs = new OrderWorkflowArgs(
   OrderId: "ORD-2026-9941",
   CustomerEmail: "Mando@beskar.dev",
   TotalCents: 49_900);

Console.WriteLine($"\n[Client] Chaining 3 source-generated handlers for Order {workflowArgs.OrderId}...");

// 1. Parent [GeneratedJobHandler]: ValidateOrderJobHandler ("orders.validate" on queue "orders")
var step1 = (await client.EnqueueGeneratedWithResultAsync<ValidateOrderJobHandler, OrderWorkflowArgs, OrderValidationResult>(workflowArgs)).Success!;

// 2. Child [GeneratedJobHandler] continuation: RenderInvoicePdfJobHandler ("billing.invoice.pdf" on queue "billing")
var step2 = (await client.EnqueueGeneratedContinuationWithResultAsync<RenderInvoicePdfJobHandler, OrderWorkflowArgs, InvoicePdfResult>(
   parentJobId: step1.JobId,
   args: workflowArgs)).Success!;

// 3. Grandchild [GeneratedJobHandler] continuation: SendReceiptEmailJobHandler ("notifications.receipt.email" on queue "notifications")
var step3 = (await client.EnqueueGeneratedContinuationAsync<SendReceiptEmailJobHandler, OrderWorkflowArgs>(
   parentJobId: step2.JobId,
   args: workflowArgs)).Success!;

Console.WriteLine($"  Stage 1 ({ValidateOrderJobHandler.JobType})          -> {step1.JobId} (Status: {step1.InitialStatus})");
Console.WriteLine($"  Stage 2 ({RenderInvoicePdfJobHandler.JobType})       -> {step2.JobId} (Status: {step2.InitialStatus})");
Console.WriteLine($"  Stage 3 ({SendReceiptEmailJobHandler.JobType}) -> {step3.JobId} (Status: {step3.InitialStatus})\n");

var validationResult = (await client.WaitForJobResultAsync<OrderValidationResult>(step1.JobId, TimeSpan.FromSeconds(5))).Success!;
Console.WriteLine($"[Client] Stage 1 Result: Approved={validationResult.Approved}, AuthCode={validationResult.AuthorizationCode} (Worker: {validationResult.WorkerId})");

var pdfResult = (await client.WaitForJobResultAsync<InvoicePdfResult>(step2.JobId, TimeSpan.FromSeconds(5))).Success!;
Console.WriteLine($"[Client] Stage 2 Result: PDF={pdfResult.DocumentPath} (Worker: {pdfResult.WorkerId})");

var emailCompletion = (await client.WaitForJobCompletionAsync(step3.JobId, TimeSpan.FromSeconds(5))).Success!;
Console.WriteLine($"[Client] Stage 3 Final Status: {emailCompletion.Status} ({emailCompletion.Message})");

await broker.StopAsync();
Console.WriteLine("\nDone.");

public sealed record OrderWorkflowArgs(string OrderId, string CustomerEmail, int TotalCents);
public sealed record OrderValidationResult(bool Approved, string AuthorizationCode, string WorkerId);
public sealed record InvoicePdfResult(string DocumentPath, string WorkerId);

[GeneratedJobHandler("orders.validate", Queue = "orders")]
public sealed partial class ValidateOrderJobHandler : IJobHandlerWithResult<OrderWorkflowArgs, OrderValidationResult>
{
   public async ValueTask<OrderValidationResult> ExecuteAsync(
      IJobExecutionContext<OrderWorkflowArgs> context,
      CancellationToken ct = default)
   {
      await context.ReportProgressAsync(50, $"Validating inventory & payment for {context.Arguments.OrderId}...", ct);
      await Task.Delay(120, ct);
      return new OrderValidationResult(true, "AUTH-88421", context.WorkerId);
   }
}

[GeneratedJobHandler("billing.invoice.pdf", Queue = "billing")]
public sealed partial class RenderInvoicePdfJobHandler : IJobHandlerWithResult<OrderWorkflowArgs, InvoicePdfResult>
{
   public async ValueTask<InvoicePdfResult> ExecuteAsync(
      IJobExecutionContext<OrderWorkflowArgs> context,
      CancellationToken ct = default)
   {
      await context.ReportProgressAsync(50, $"Rendering invoice PDF for {context.Arguments.OrderId}...", ct);
      await Task.Delay(120, ct);
      return new InvoicePdfResult($"/invoices/{context.Arguments.OrderId}.pdf", context.WorkerId);
   }
}

[GeneratedJobHandler("notifications.receipt.email", Queue = "notifications")]
public sealed partial class SendReceiptEmailJobHandler : IJobHandler<OrderWorkflowArgs>
{
   public async ValueTask ExecuteAsync(
      IJobExecutionContext<OrderWorkflowArgs> context,
      CancellationToken ct = default)
   {
      await context.ReportProgressAsync(75, $"Sending receipt email to {context.Arguments.CustomerEmail}...", ct);
      await Task.Delay(100, ct);
      Console.WriteLine($"[Worker '{context.WorkerId}'] Sent receipt for {context.Arguments.OrderId} to {context.Arguments.CustomerEmail}");
   }
}
