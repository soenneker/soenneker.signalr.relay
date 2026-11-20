using System.Text.RegularExpressions;

namespace Soenneker.Cloudflare.SignalR;

internal sealed partial class SubscriberFilter
{
    private readonly string[] _tokens;
    private int _at;

    private SubscriberFilter(string[] tokens) => _tokens = tokens;

    [GeneratedRegex(@"\G\s*('[^']*(?:''[^']*)*'|[a-zA-Z][a-zA-Z0-9]*|[(),])", RegexOptions.CultureInvariant, 50)]
    private static partial Regex Lexer();

    public static Func<RelayClient, bool>? Parse(string expression)
    {
        if (expression.Length > 2048) return null;
        try
        {
            var tokens = new List<string>();
            var text = expression.Trim();
            int consumed = 0;
            foreach (Match match in Lexer().Matches(text))
            {
                if (tokens.Count >= 96) return null;
                tokens.Add(match.Groups[1].Value);
                consumed += match.Length;
            }
            if (consumed != text.Length) return null;
            var parser = new SubscriberFilter(tokens.ToArray());
            var predicate = parser.Or();
            return parser._at == tokens.Count ? predicate : null;
        }
        catch (FormatException) { return null; }
        catch (RegexMatchTimeoutException) { return null; }
    }

    private Func<RelayClient, bool> Or()
    {
        var predicate = And();
        while (Take("or"))
        {
            var left = predicate;
            var right = And();
            predicate = client => left(client) || right(client);
        }
        return predicate;
    }

    private Func<RelayClient, bool> And()
    {
        var predicate = Term();
        while (Take("and"))
        {
            var left = predicate;
            var right = Term();
            predicate = client => left(client) && right(client);
        }
        return predicate;
    }

    private Func<RelayClient, bool> Term()
    {
        if (Take("not"))
        {
            var child = Term();
            return client => !child(client);
        }
        if (Take("("))
        {
            var child = Or();
            Require(")");
            return child;
        }
        var operation = Next();
        if (operation.StartsWith("'", StringComparison.Ordinal))
        {
            Require("in");
            Require("groups");
            string group = Unquote(operation);
            return client => client.Groups.ContainsKey(group);
        }
        if (operation is "startswith" or "endswith" or "contains")
        {
            Require("(");
            var field = Field(Next());
            Require(",");
            var value = Literal();
            Require(")");
            return operation switch
            {
                "startswith" => client => field(client)?.StartsWith(value, StringComparison.Ordinal) == true,
                "endswith" => client => field(client)?.EndsWith(value, StringComparison.Ordinal) == true,
                _ => client => field(client)?.Contains(value, StringComparison.Ordinal) == true
            };
        }
        var get = Field(operation);
        var comparison = Next();
        var literal = Take("null") ? null : Literal();
        return comparison switch
        {
            "eq" => client => get(client) == literal,
            "ne" => client => get(client) != literal,
            _ => throw new FormatException()
        };
    }

    private static Func<RelayClient, string?> Field(string name) => name switch
    {
        "userId" => client => client.UserId,
        "connectionId" => client => client.Peer.Id,
        _ => throw new FormatException()
    };

    private string Literal()
    {
        var token = Next();
        if (!token.StartsWith("'", StringComparison.Ordinal)) throw new FormatException();
        return Unquote(token);
    }

    private static string Unquote(string token) => token[1..^1].Replace("''", "'");

    private string Next() => _at < _tokens.Length ? _tokens[_at++] : throw new FormatException();

    private bool Take(string value)
    {
        if (_at == _tokens.Length || _tokens[_at] != value) return false;
        _at++;
        return true;
    }

    private void Require(string value)
    {
        if (!Take(value)) throw new FormatException();
    }
}
