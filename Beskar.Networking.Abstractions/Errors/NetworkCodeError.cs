namespace Beskar.Networking.Abstractions.Errors;

public readonly record struct NetworkCodeError(
   int Code, string Message);