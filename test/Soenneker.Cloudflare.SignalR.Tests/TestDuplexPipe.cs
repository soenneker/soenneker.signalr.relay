using System.IO.Pipelines;

namespace Soenneker.Cloudflare.SignalR.Tests;

internal sealed record TestDuplexPipe(PipeReader Input, PipeWriter Output) : IDuplexPipe;
