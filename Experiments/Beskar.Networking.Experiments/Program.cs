using System;
using System.Reflection;
using Beskar.Memory.Results;
using Beskar.Networking.Abstractions.Interfaces;
using Beskar.Networking.Abstractions.Errors;

var type = typeof(Result<INetworkSession, NetworkCodeError>);
Console.WriteLine($"Type: {type.FullName}");

Console.WriteLine("--- Constructors ---");
foreach (var ctor in type.GetConstructors(BindingFlags.Public | BindingFlags.Instance | BindingFlags.NonPublic))
{
    Console.WriteLine(ctor.ToString());
}

Console.WriteLine("--- Methods ---");
foreach (var method in type.GetMethods(BindingFlags.Public | BindingFlags.Static | BindingFlags.Instance))
{
    Console.WriteLine(method.ToString());
}

Console.WriteLine("--- Fields ---");
foreach (var field in type.GetFields(BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Instance | BindingFlags.Static))
{
    Console.WriteLine(field.ToString());
}
