namespace Beskar.Utilities.Console.Rendering;

public static partial class ConsoleRender
{
    private static readonly string[] SpinnerFrames = ["⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"];

    /// <summary>
    /// Runs an asynchronous void task while displaying a beautiful console spinner.
    /// </summary>
    public static async Task RunSpinnerAsync(
        string message,
        Func<Task> action,
        string? successMessage = null,
        string? errorMessage = null)
    {
        await RunSpinnerAsync<object?>(message, async () =>
        {
            await action();
            return null;
        }, successMessage, errorMessage);
    }

    /// <summary>
    /// Runs an asynchronous value task while displaying a beautiful console spinner.
    /// </summary>
    public static async Task<T> RunSpinnerAsync<T>(
        string message,
        Func<Task<T>> action,
        string? successMessage = null,
        string? errorMessage = null)
    {
        var cts = new CancellationTokenSource();
        var spinnerTask = Task.Run(() => PlaySpinner(message, cts.Token), cts.Token);

        try
        {
            var result = await action();
            await cts.CancelAsync();

            try
            {
                await spinnerTask;
            }
            catch { /* Ignored */ }

            ClearCurrentLine();
            Success(successMessage ?? $"{message} completed.");
            return result;
        }
        catch (Exception ex)
        {
            await cts.CancelAsync();

            try
            {
                await spinnerTask;
            }
            catch { /* Ignored */ }

            ClearCurrentLine();
            Error(errorMessage ?? $"{message} failed: {ex.Message}");
            throw;
        }
        finally
        {
            cts.Dispose();
        }
    }

    private static async Task PlaySpinner(string message, CancellationToken token)
    {
        var originalColor = System.Console.ForegroundColor;
        var cursorVisibleBefore = true;

        if (OperatingSystem.IsWindows())
        {
            try
            {
                cursorVisibleBefore = System.Console.CursorVisible;
                System.Console.CursorVisible = false;
            }
            catch { /* Terminal may not support cursor operations */ }
        }

        var frameIndex = 0;
        try
        {
            while (!token.IsCancellationRequested)
            {
                var frame = SpinnerFrames[frameIndex];

                System.Console.Write("\r");
                System.Console.ForegroundColor = ConsoleColor.Yellow;
                System.Console.Write(frame);
                System.Console.ForegroundColor = ConsoleColor.DarkGray;
                System.Console.Write(" ");
                System.Console.ForegroundColor = originalColor;
                System.Console.Write(message);

                frameIndex = (frameIndex + 1) % SpinnerFrames.Length;
                await Task.Delay(80, token);
            }
        }
        catch (TaskCanceledException)
        {
            // Expected cancellation
        }
        finally
        {
            System.Console.ForegroundColor = originalColor;
            if (OperatingSystem.IsWindows())
            {
                try
                {
                    System.Console.CursorVisible = cursorVisibleBefore;
                }
                catch
                {
                   // ignored
                }
            }
        }
    }

    private static void ClearCurrentLine()
    {
        try
        {
            System.Console.Write($"\r{new string(' ', System.Console.WindowWidth - 1)}\r");
        }
        catch
        {
            System.Console.Write("\r                                                                                \r");
        }
    }
}
