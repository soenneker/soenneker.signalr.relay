using System.Text.RegularExpressions;

namespace Soenneker.SignalR.Relay;

/// <summary>Parses subscriber filter expressions into predicates over attached relay clients.</summary>
internal sealed partial class SubscriberFilter
{
    /// <summary>Tokens from the validated expression.</summary>
    private readonly string[] _tokens;

    /// <summary>Index of the next unconsumed token.</summary>
    private int _at;

    /// <summary>Starts parsing at the first token.</summary>
    /// <param name="tokens">Tokens in expression order.</param>
    private SubscriberFilter(string[] tokens) => _tokens = tokens;

    /// <summary>Matches one contiguous filter token and its leading whitespace.</summary>
    /// <returns>The regular expression used to tokenize filter expressions.</returns>
    [GeneratedRegex(@"\G\s*('[^']*(?:''[^']*)*'|[a-zA-Z][a-zA-Z0-9]*|[(),])", RegexOptions.CultureInvariant, 50)]
    private static partial Regex Lexer();

    /// <summary>Compiles a bounded filter expression for multicast delivery.</summary>
    /// <param name="expression">Subscriber filter sent with the multicast message.</param>
    /// <returns>The client predicate, or null for invalid or unsupported input.</returns>
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

    /// <summary>Parses a disjunction of higher-precedence expressions.</summary>
    /// <returns>A predicate that evaluates the parsed disjunction.</returns>
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

    /// <summary>Parses a conjunction of filter terms.</summary>
    /// <returns>A predicate that evaluates the parsed conjunction.</returns>
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

    /// <summary>Parses a negation, grouped expression, membership test, or field comparison.</summary>
    /// <returns>A predicate for the parsed term.</returns>
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

    /// <summary>Resolves a supported filter field to its client value accessor.</summary>
    /// <param name="name">Filter field name.</param>
    /// <returns>An accessor for the selected client field.</returns>
    private static Func<RelayClient, string?> Field(string name) => name switch
    {
        "userId" => client => client.UserId,
        "connectionId" => client => client.Peer.Id,
        _ => throw new FormatException()
    };

    /// <summary>Consumes a quoted string literal and decodes doubled apostrophes.</summary>
    /// <returns>The unescaped literal value.</returns>
    private string Literal()
    {
        var token = Next();
        if (!token.StartsWith("'", StringComparison.Ordinal)) throw new FormatException();
        return Unquote(token);
    }

    /// <summary>Removes surrounding quotes and decodes escaped apostrophes.</summary>
    /// <param name="token">Quoted filter token.</param>
    /// <returns>The unescaped token contents.</returns>
    private static string Unquote(string token) => token[1..^1].Replace("''", "'");

    /// <summary>Consumes the next token or reports an incomplete expression.</summary>
    /// <returns>The token at the previous cursor position.</returns>
    private string Next() => _at < _tokens.Length ? _tokens[_at++] : throw new FormatException();

    /// <summary>Consumes the next token only if it matches the expected value.</summary>
    /// <param name="value">Expected filter token.</param>
    /// <returns>True if the expected token was consumed.</returns>
    private bool Take(string value)
    {
        if (_at == _tokens.Length || _tokens[_at] != value) return false;
        _at++;
        return true;
    }

    /// <summary>Requires the next token to match the supplied value.</summary>
    /// <param name="value">Expected filter token.</param>
    private void Require(string value)
    {
        if (!Take(value)) throw new FormatException();
    }
}
