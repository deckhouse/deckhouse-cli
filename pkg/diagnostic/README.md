# pkg/diagnostic

User-friendly error diagnostics for d8 CLI. Known errors get formatted
with possible causes and solutions instead of raw Go error text:

```
error: TLS/certificate verification failed
  ╰─▶ x509: certificate signed by unknown authority

  * Self-signed certificate on the source registry
    -> Use --tls-skip-verify flag to skip TLS verification
  * Corporate proxy intercepting HTTPS connections
    -> Check if a corporate proxy is intercepting HTTPS traffic
```

## How it works

```
  root.go                              mirror/cmd/pull (RunE)
  ───────                              ──────────────────────

  rootCmd.Execute()
       |
       |  cobra dispatches
       |  to subcommand ──────────────> err := puller.Execute()
       |                                     |
       |                                [Diagnose err] -> is it HelpfulError?
       |                                     |
       |                                 yes | no
       |                                  |     |
       |                  *HelpfulError <-+     +-> fmt.Errorf("pull failed: %w", err)
       |                                  |     |
       |  error returns   <───────────────+─────+
       |
  [errors.As HelpfulError?]
       |
   yes | no
    |     |
    v     v
  .Format()   "Error executing command: ..." (as usual)
  (colored)   (plain)
```

Each command diagnoses errors with its own errdetect package.
`root.go` only catches `*HelpfulError` via `errors.As` - it does not
import any errdetect, so unrelated commands never get false diagnostics.

## HelpfulError

```go
type Suggestion struct {
    Cause     string   // why it might have happened
    Solutions []string // how to fix this specific cause
}

type HelpfulError struct {
    Category    string       // what went wrong: "TLS/certificate verification failed"
    OriginalErr error        // the underlying error (required, used by Unwrap/Error/Format)
    Suggestions []Suggestion // cause-solution pairs (optional)
}
```

| Field | Required | What happens if empty |
|-------|----------|----------------------|
| `Category` | yes | output shows `error: ` with no description |
| `OriginalErr` | yes | safe (no panic), but `Unwrap` returns nil and `Format` skips the error line |
| `Suggestions` | no | suggestions section is omitted |

How fields map to output (`Format()`):

```
error: TLS/certificate verification failed  <-- Category
  ╰─▶ x509: certificate signed by ...      <-- OriginalErr (unwrapped chain)

  * Self-signed certificate                 <-- Suggestion.Cause
    -> Use --tls-skip-verify flag           <-- Suggestion.Solutions
  * Corporate proxy intercepting HTTPS      <-- next Suggestion.Cause
    -> Check if proxy is intercepting ...   <-- its Solutions
```

`Error()` returns plain text for logs: `"Category: OriginalErr.Error()"`.
`Unwrap()` returns `OriginalErr` so `errors.Is`/`errors.As` work through it.

## Where classifiers live

Classifiers are **application/UI logic**, not library code. They contain
user-facing advice (CLI flags, links to docs) that is specific to each command.
Place them in `internal/` next to the command they serve.

```
pkg/diagnostic/                        HelpfulError + Format (generic, reusable)
pkg/registry/errmatch/                 error matchers (generic, reusable)
internal/mirror/cmd/pull/errdetect/    pull-specific diagnostics
internal/mirror/cmd/push/errdetect/    push-specific diagnostics
```

Why per-command: pull advises `--license`/`--source-login`, push advises
`--registry-login`/`--registry-password`. Shared classifier would give
ambiguous advice.

## Adding diagnostics to a new command

**1. Create an errdetect package** next to your command:

```go
// internal/backup/cmd/snapshot/errdetect/diagnose.go
package errdetect

import (
    "errors"
    "github.com/deckhouse/deckhouse-cli/pkg/diagnostic"
)

func Diagnose(err error) *diagnostic.HelpfulError {
    var helpErr *diagnostic.HelpfulError
    if errors.As(err, &helpErr) {
        return nil // already diagnosed, don't wrap twice
    }

    if isETCDError(err) {
        return &diagnostic.HelpfulError{
            Category:    "ETCD connection failed",
            OriginalErr: err,
            Suggestions: []diagnostic.Suggestion{
                {
                    Cause:     "ETCD cluster is unreachable",
                    Solutions: []string{"Check ETCD health: etcdctl endpoint health"},
                },
            },
        }
    }
    return nil
}
```

**2. Call it in RunE** of your leaf command:

```go
if err := doSnapshot(); err != nil {
    if diag := errdetect.Diagnose(err); diag != nil {
        return diag
    }
    return fmt.Errorf("snapshot failed: %w", err)
}
```

No changes to `root.go` needed - it catches any `*HelpfulError`
regardless of which errdetect produced it.

## Rules (Best Practice)

- Classifiers go in `internal/<command>/errdetect/` - they are application logic, not libraries
- Diagnose in the **leaf command** (RunE), not in libraries or root.go
- Each command uses its **own errdetect** - prevents false diagnostics
- Skip diagnosis if the error is already a `*HelpfulError` (see guard in the example above)
- `Suggestions` are optional but highly recommended
