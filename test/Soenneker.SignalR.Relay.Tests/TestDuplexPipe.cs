using System.IO.Pipelines;

namespace Soenneker.SignalR.Relay.Tests;

internal sealed record TestDuplexPipe(PipeReader Input, PipeWriter Output) : IDuplexPipe;
