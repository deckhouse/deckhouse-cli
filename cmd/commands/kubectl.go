/*
Copyright 2024 Flant JSC

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package commands

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/signal"
	"regexp"
	"strings"
	"syscall"
	"time"

	"github.com/spf13/cobra"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/cli-runtime/pkg/genericclioptions"
	"k8s.io/cli-runtime/pkg/genericiooptions"
	"k8s.io/client-go/discovery"
	"k8s.io/client-go/discovery/cached/memory"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/restmapper"
	cliflag "k8s.io/component-base/cli/flag"
	"k8s.io/component-base/logs"
	"k8s.io/klog/v2"
	kubecmd "k8s.io/kubectl/pkg/cmd"
	"k8s.io/kubectl/pkg/cmd/plugin"
	cmdutil "k8s.io/kubectl/pkg/cmd/util"
)

const (
	cmNamespace = "d8-system"
	cmName      = "debug-container"
	cmImageKey  = "image"
)

// Header names the kube-apiserver ACL-filtering patch reads to resolve
// --scope. Kept in sync by hand with
// k8s.io/apiserver/pkg/registry/generic/scopefilter.HeaderScope/HeaderProject
// in the deckhouse/deckhouse kubernetes patch -- there is no shared Go
// package between the two repos to import this from.
const (
	scopeHeaderName        = "X-Deckhouse-Scope"
	scopeProjectHeaderName = "X-Deckhouse-Project"
)

// scopeFlagName intentionally matches the unrelated --scope flag already
// registered on `d8 iam access grant/revoke` (different enum: cluster,
// all-namespaces, labels=K=V). They live on disjoint cobra command trees, so
// there is no flag-registration conflict, but a user pasting a value meant
// for one into the other must get a clear "invalid value" error rather than
// having it silently misinterpreted -- see parseScopeFlag.
const scopeFlagName = "scope"

// scopeProjectValuePrefix is the flag-value spelling of the single-project
// scope: `--scope=project:<name>`.
const scopeProjectValuePrefix = "project:"

// parseScopeFlag validates raw against the accessible|projects|system|project:<name>
// enum and splits it into the two wire headers scopeHeaderName expects: the
// scope kind and the project name (non-empty only for the project:<name>
// form). Returns ("", "", nil) for an empty raw value -- the flag was not
// passed at all, and callers must send no headers whatsoever in that case, so
// a client without --scope stays byte-for-byte identical to plain kubectl.
func parseScopeFlag(raw string) (string, string, error) {
	if raw == "" {
		return "", "", nil
	}

	if projectName, ok := strings.CutPrefix(raw, scopeProjectValuePrefix); ok {
		if projectName == "" {
			return "", "", fmt.Errorf("--%s=project: requires a project name, e.g. --%s=project:myproject", scopeFlagName, scopeFlagName)
		}

		return "project", projectName, nil
	}

	switch raw {
	case "accessible", "projects", "system":
		return raw, "", nil
	default:
		return "", "", fmt.Errorf("invalid --%s value %q: must be one of accessible|projects|system|project:<name>", scopeFlagName, raw)
	}
}

// scopeUsageError enforces that --scope is only used where it can actually
// work. --scope filters a cross-namespace listing, which needs the
// cluster-scoped request URL that -A/--all-namespaces produces; without -A the
// request is locked to a single namespace and --scope could only ever narrow
// that one namespace (usually to empty), silently turning a clean 403 into a
// misleading empty result. An explicit -n/--namespace has the same problem, so
// both degenerate forms are refused up front with an actionable message.
// Returns nil when scopeValue is empty (flag not passed) or the usage is valid.
func scopeUsageError(cmd *cobra.Command, scopeValue string) error {
	if scopeValue == "" {
		return nil
	}
	// Check the -n conflict first: when the user explicitly passed -n, that is
	// the more specific mistake, and reporting "requires -A" would just send them
	// into the -n error on the retry.
	if f := cmd.Flag("namespace"); f != nil && f.Changed {
		return fmt.Errorf("--%s cannot be combined with -n/--namespace; use -A/--all-namespaces to list across namespaces", scopeFlagName)
	}

	// Check the VALUE, not flag.Changed: an explicit --all-namespaces=false
	// marks the flag as changed while leaving the request namespaced, which is
	// exactly the degenerate case this guard exists to refuse.
	if f := cmd.Flag("all-namespaces"); f == nil || f.Value.String() != "true" {
		return fmt.Errorf("--%s requires -A/--all-namespaces", scopeFlagName)
	}

	return nil
}

// scopeForbiddenHint returns the hint to print right after msg, or "" when
// msg is not the cluster-scope Forbidden error or the invocation could not
// have used --scope anyway (eligible is false).
//
// eligible and resource are captured at PersistentPreRunE time by
// NewKubectlCommand and describe the current invocation: eligible means "a
// `get` with a true -A and no --scope" -- one --scope could have helped;
// resource is whatever resource argument the user actually typed (e.g.
// "pods", "deployments", "pods,svc"), or "" when it could not be determined,
// in which case the examples fall back to a <resource> placeholder rather
// than guessing.
//
// The wording is conditional ("If this is a namespaced resource") on
// purpose: a 403 for a cluster-scoped resource (e.g. `get nodes -A` --
// kubectl accepts and ignores -A there) matches the same substrings, but
// --scope cannot help it -- the server filters namespaces and cluster-scoped
// resources live in none. The resource's scope cannot be determined from the
// message text, and resolving it via discovery inside a fatal handler would
// trade a misleading sentence for a possible hang, so the text hedges
// instead.
func scopeForbiddenHint(msg string, eligible bool, resource string) string {
	if !eligible {
		return ""
	}

	if !strings.Contains(msg, "is forbidden:") || !strings.Contains(msg, "at the cluster scope") {
		return ""
	}

	if resource == "" {
		resource = "<resource>"
	}

	return fmt.Sprintf(`
You don't have permission to list this resource at the cluster scope. If
this is a namespaced resource, the Deckhouse platform can return just the
subset you CAN access -- opt in with the --scope flag:

  d8 k get %[1]s -A --scope=accessible       # every namespace your RBAC grants
  d8 k get %[1]s -A --scope=projects         # all namespaces of your projects
  d8 k get %[1]s -A --scope=project:<name>   # a single project's namespaces
  d8 k get %[1]s -A --scope=system           # non-project (system) namespaces

(For cluster-scoped resources, e.g. nodes, --scope does not apply.)
`, resource)
}

// scopeHeaderRoundTripper injects the two scope wire headers into every
// outgoing request. Wraps (rather than replaces) whatever RoundTripper it's
// given so it composes with exec-credential auth plugins and any other
// transport wrapping already in the chain -- see wrapConfigWithScope.
type scopeHeaderRoundTripper struct {
	rt      http.RoundTripper
	kind    string
	project string
}

func (t *scopeHeaderRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	req = req.Clone(req.Context())
	req.Header.Set(scopeHeaderName, t.kind)

	if t.project != "" {
		req.Header.Set(scopeProjectHeaderName, t.project)
	}

	return t.rt.RoundTrip(req)
}

// wrapConfigWithScope returns a copy of c whose transport injects the scope
// headers for (kind, project) into every request. Composes with any
// pre-existing WrapTransport (e.g. from exec-credential auth) by nesting our
// RoundTripper OUTSIDE it: headers are set first, then the request is handed
// down to whatever auth/transport layers were already configured -- they add
// their own headers (e.g. Authorization) and pass the request through
// unaffected by our unrelated X-Deckhouse-* ones.
func wrapConfigWithScope(c *rest.Config, kind, project string) *rest.Config {
	cfg := rest.CopyConfig(c)
	previous := cfg.WrapTransport
	cfg.WrapTransport = func(rt http.RoundTripper) http.RoundTripper {
		if previous != nil {
			rt = previous(rt)
		}

		return &scopeHeaderRoundTripper{rt: rt, kind: kind, project: project}
	}

	return cfg
}

// scopeStaticValues are the non-project --scope enum values.
var scopeStaticValues = []string{"accessible", "projects", "system"}

// scopeCompletionTimeout bounds every network round-trip shell completion may
// make (discovery and the Project LIST alike). Completion runs on a tab
// press: past a few seconds the user has long stopped waiting.
const scopeCompletionTimeout = 3 * time.Second

// scopeCompletion provides shell completion for --scope. It offers the static
// enum values, and -- once the user has typed the `project:` prefix -- a
// `project:<name>` candidate for each real project. Completion is
// best-effort: any failure to enumerate projects yields no dynamic candidates
// rather than an error (the user can always type the name).
//
// The cluster is contacted ONLY when toComplete already starts with
// "project:". For anything shorter (a bare <TAB>, "pro", ...) a literal
// `project:` candidate is offered instead: the static values need no cluster
// at all, and a slow or unreachable cluster must not freeze the shell for a
// user who pressed tab just to see what the flag accepts. Completing the
// literal then naturally leads to a second tab press with the full prefix,
// and only that one fetches real names.
//
// configFlags is the SAME instance the kubectl command tree parses its
// connection flags into, so completion talks to whatever cluster the actual
// command would: --server, --token, --as, --cluster, --request-timeout and
// friends are all honored, not just --kubeconfig/--context.
func scopeCompletion(configFlags *genericclioptions.ConfigFlags, toComplete string) ([]string, cobra.ShellCompDirective) {
	var out []string

	for _, v := range scopeStaticValues {
		if strings.HasPrefix(v, toComplete) {
			out = append(out, v)
		}
	}

	if strings.HasPrefix(toComplete, scopeProjectValuePrefix) {
		for _, name := range fetchProjectNames(configFlags) {
			if candidate := scopeProjectValuePrefix + name; strings.HasPrefix(candidate, toComplete) {
				out = append(out, candidate)
			}
		}
	} else if strings.HasPrefix(scopeProjectValuePrefix, toComplete) {
		// toComplete is a (possibly empty) prefix of "project:" -- the user
		// may be typing toward the project form. Offer the literal without
		// touching the cluster.
		out = append(out, scopeProjectValuePrefix)
	}

	return out, cobra.ShellCompDirectiveNoFileComp
}

// fetchProjectNames lists Deckhouse Project names for completion. The Project
// resource's served version is bumped over time, so the version is resolved
// dynamically via a discovery RESTMapper (the group "deckhouse.io" and Kind
// "Project" are the stable parts) instead of being hardcoded -- a hardcoded
// version would silently stop completing after an upgrade.
//
// Every request -- discovery included -- runs on a client whose Timeout is
// capped at scopeCompletionTimeout, so completion cannot hang the shell on an
// unreachable cluster. An explicit shorter --request-timeout from the user is
// respected (it arrives via configFlags.ToRESTConfig).
func fetchProjectNames(configFlags *genericclioptions.ConfigFlags) []string {
	restConfig, err := configFlags.ToRESTConfig()
	if err != nil {
		return nil
	}

	cfg := rest.CopyConfig(restConfig)
	if cfg.Timeout == 0 || cfg.Timeout > scopeCompletionTimeout {
		cfg.Timeout = scopeCompletionTimeout
	}

	// Build the RESTMapper from the SAME time-bounded config: ToRESTMapper's
	// discovery would otherwise run outside any deadline and could block far
	// longer than the LIST below.
	disc, err := discovery.NewDiscoveryClientForConfig(cfg)
	if err != nil {
		return nil
	}

	mapper := restmapper.NewDeferredDiscoveryRESTMapper(memory.NewMemCacheClient(disc))

	mapping, err := mapper.RESTMapping(schema.GroupKind{Group: "deckhouse.io", Kind: "Project"})
	if err != nil {
		return nil // Project CRD absent or discovery failed -- no dynamic candidates
	}

	dyn, err := dynamic.NewForConfig(cfg)
	if err != nil {
		return nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), scopeCompletionTimeout)
	defer cancel()

	list, err := dyn.Resource(mapping.Resource).List(ctx, v1.ListOptions{})
	if err != nil {
		return nil // no rights / unreachable -- best-effort, stay silent
	}

	names := make([]string, 0, len(list.Items))
	for i := range list.Items {
		names = append(names, list.Items[i].GetName())
	}

	return names
}

var d8CommandRegex = regexp.MustCompile("([\"'`])d8 (\\w+)")

// rewriteD8Commands rewrites kubectl's quoted "d8 <subcmd>" references to
// "d8 k <subcmd>". kubectl builds such messages two ways: from os.Args[0]
// ("d8 get ..."), which needs the insertion, and from cmd.CommandPath()
// ("d8 k get ..."), which is already correct -- inserting there would produce
// "d8 k k get". RE2 has no lookahead, so the skip is done in a replace
// callback: a match whose subcommand is already `k` is left untouched. Every
// rewrite path (the IOStreams writer, wrapRunE, the fatal handler) must go
// through this helper so they can't drift apart.
func rewriteD8Commands(s string) string {
	return d8CommandRegex.ReplaceAllStringFunc(s, func(m string) string {
		sub := d8CommandRegex.FindStringSubmatch(m)
		if sub[2] == "k" {
			return m
		}

		return sub[1] + "d8 k " + sub[2]
	})
}

// d8KubectlWriter wraps an io.Writer and rewrites kubectl's "d8 <subcmd>"
// references to "d8 k <subcmd>" on each Write call (see rewriteD8Commands).
//
// kubectl uses os.Args[0] as the displayed command name in user-facing
// suggestions (e.g. "You can run `d8 replace -f ...` to try this update again.").
// Since the binary is invoked as "d8", those suggestions point users to a
// non-existent top-level command. Wrapping IOStreams.ErrOut with this writer
// ensures the suggestions are rewritten to the correct "d8 k <subcmd>" form
// before reaching the terminal.
type d8KubectlWriter struct {
	w io.Writer
}

func newD8KubectlWriter(w io.Writer) *d8KubectlWriter {
	return &d8KubectlWriter{w: w}
}

func (d *d8KubectlWriter) Write(p []byte) (int, error) {
	rewritten := rewriteD8Commands(string(p))
	if _, err := d.w.Write([]byte(rewritten)); err != nil {
		return 0, err
	}
	// Report the original input length to honor the io.Writer contract even
	// though the rewritten payload may have a different byte length.
	return len(p), nil
}

// wrapRunE wraps all RunE functions in the kubectl command tree to intercept stderr output.
// This approach is preferred over modifying os.Args[0] because:
// - It avoids modifying global state (os.Args) which could affect other parts of the system
// - It provides surgical precision, only affecting kubectl error messages
// - It preserves the integrity of os.Args for logging, debugging, and other tools
// - It maintains clean separation of concerns without side effects
func wrapRunE(cmd *cobra.Command) {
	if cmd.RunE != nil {
		originalRunE := cmd.RunE

		cmd.RunE = func(cmd *cobra.Command, args []string) error {
			// Create a pipe to capture stderr
			r, w, err := os.Pipe()
			if err != nil {
				return originalRunE(cmd, args)
			}

			oldStderr := os.Stderr
			os.Stderr = w

			err = originalRunE(cmd, args)

			w.Close()

			os.Stderr = oldStderr

			// Read the captured output
			output, readErr := io.ReadAll(r)
			r.Close()

			if readErr != nil {
				return err
			}

			// Modify the output to fix the command suggestion
			modifiedOutput := rewriteD8Commands(string(output))

			// Write the modified output to real stderr
			fmt.Fprint(oldStderr, modifiedOutput)

			return err
		}
	}

	for _, sub := range cmd.Commands() {
		wrapRunE(sub)
	}
}

func getDebugImage(cmd *cobra.Command) (string, error) {
	configFlags := genericclioptions.NewConfigFlags(true)
	if val, err := cmd.InheritedFlags().GetString("kubeconfig"); err == nil {
		configFlags.KubeConfig = &val
	}

	if val, err := cmd.InheritedFlags().GetString("context"); err == nil {
		configFlags.Context = &val
	}

	restConfig, err := configFlags.ToRESTConfig()
	if err != nil {
		return "", fmt.Errorf("Failed to create Kubernetes client: %w", err)
	}

	kubeCl, err := kubernetes.NewForConfig(restConfig)
	if err != nil {
		return "", fmt.Errorf("Failed to create Kubernetes client: %w", err)
	}

	var ErrGenericImageFetch = errors.New("Cannot get debug image from cluster, please specify --image explicitly")

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	configMap, err := kubeCl.CoreV1().ConfigMaps(cmNamespace).Get(ctx, cmName, v1.GetOptions{})
	if err != nil {
		return "", ErrGenericImageFetch
	}

	imageName, ok := configMap.Data[cmImageKey]
	if !ok || imageName == "" {
		return "", ErrGenericImageFetch
	}

	return imageName, nil
}

func NewKubectlCommand() *cobra.Command {
	// Build a kubectl command tree with stderr wrapped by d8KubectlWriter so
	// kubectl's "d8 <subcmd>" command hints are rewritten to "d8 k <subcmd>".
	//
	// This must be applied at construction time: kubectl captures os.Stderr
	// once into IOStreams.ErrOut and stores the reference (see
	// k8s.io/kubectl/pkg/cmd/cmd.go:NewDefaultKubectlCommand). Code paths that
	// write via that stored reference (e.g. the post-edit hint
	// "You can run `d8 replace -f ...`" emitted from editoptions.go) cannot be
	// intercepted by later swaps of the os.Stderr global.
	ioStreams := genericiooptions.IOStreams{
		In:     os.Stdin,
		Out:    os.Stdout,
		ErrOut: newD8KubectlWriter(os.Stderr),
	}

	// scopeKind/scopeProject are populated in PersistentPreRunE below, once
	// cobra has parsed --scope from argv -- WithWrapConfigFn's closure reads
	// them lazily at ToRESTConfig() time, which every subcommand only reaches
	// after PersistentPreRunE has already run.
	var scopeKind, scopeProject string

	// scopeHintEligible/scopeHintResource capture, at PersistentPreRunE time,
	// whether the current invocation is one --scope could have helped (a `get`
	// with a true -A and no --scope) and which resource it asked for. Locals
	// deliberately: both users -- PersistentPreRunE and the BehaviorOnFatal
	// closure -- are defined in this function, so command trees built by
	// separate NewKubectlCommand calls cannot leak hint state into each other.
	var (
		scopeHintEligible bool
		scopeHintResource string
	)

	// configFlags is shared between the kubectl command tree (cobra parses all
	// connection flags straight into it) and the --scope shell completion,
	// which therefore talks to the same cluster the actual command would.
	configFlags := genericclioptions.NewConfigFlags(true).
		WithDeprecatedPasswordFlag().
		WithDiscoveryBurst(300).
		WithDiscoveryQPS(50.0).
		WithWarningPrinter(ioStreams).
		WithWrapConfigFn(func(c *rest.Config) *rest.Config {
			if scopeKind == "" {
				return c
			}

			return wrapConfigWithScope(c, scopeKind, scopeProject)
		})

	kubectlCmd := kubecmd.NewDefaultKubectlCommandWithArgs(kubecmd.KubectlOptions{
		PluginHandler: kubecmd.NewDefaultPluginHandler(plugin.ValidPluginFilenamePrefixes),
		Arguments:     os.Args,
		ConfigFlags:   configFlags,
		IOStreams:     ioStreams,
	})
	kubectlCmd.Use = "k"
	kubectlCmd.Aliases = []string{"kubectl"}
	kubectlCmd = ReplaceCommandName("kubectl", "d8 k", kubectlCmd)

	// scopeFlagValue is bound to `--scope` on the `get` command only (registered
	// in the command loop below), NOT as a persistent flag on the `k` root. The
	// server only ACL-filters list/get/watch of top-level resources, which in
	// kubectl terms is `get` (and `get -w`). On other subcommands the header is
	// either ignored (top hits metrics.k8s.io; delete/patch are not bypassed;
	// logs/exec are subresources) or actively misleading (describe's internal
	// LIST would be silently filtered), so `--scope` must not be offered there.
	var scopeFlagValue string

	// Fallback rewrite for kubectl code paths that write to the global
	// os.Stderr directly instead of using IOStreams.ErrOut.
	wrapRunE(kubectlCmd)

	// kubectl surfaces API errors through cmdutil.CheckErr -> fatal, which
	// prints to os.Stderr and calls os.Exit directly -- bypassing both the
	// IOStreams writer and wrapRunE above. Override it to (a) run the same
	// "d8 <subcmd>" -> "d8 k <subcmd>" rewrite on fatal messages, and (b)
	// append the --scope hint when a `get -A` without --scope dies with the
	// cluster-scope Forbidden error (see scopeForbiddenHint). Otherwise this
	// replicates the default handler byte-for-byte, including the klog.V(99)
	// debugging path (`d8 k <cmd> -v=99` panics through klog with stack
	// traces, the stock way to find where a fatal error originates).
	cmdutil.BehaviorOnFatal(func(msg string, code int) {
		if klog.V(99).Enabled() {
			klog.FatalDepth(2, msg)
		}

		if len(msg) > 0 {
			if !strings.HasSuffix(msg, "\n") {
				msg += "\n"
			}

			msg = rewriteD8Commands(msg)
			fmt.Fprint(os.Stderr, msg)

			if hint := scopeForbiddenHint(msg, scopeHintEligible, scopeHintResource); hint != "" {
				fmt.Fprint(os.Stderr, hint)
			}
		}

		os.Exit(code)
	})

	var (
		debugCmd *cobra.Command
		getCmd   *cobra.Command
	)

	for _, cmd := range kubectlCmd.Commands() {
		switch cmd.Name() {
		case "debug":
			debugCmd = cmd
		case "get":
			getCmd = cmd
		}
	}

	if getCmd != nil {
		getCmd.Flags().StringVar(&scopeFlagValue, scopeFlagName, "",
			"Cross-namespace listing scope for users without cluster-wide list/watch RBAC: "+
				"accessible (everything RBAC grants you), projects, system, or project:<name>. "+
				"Requires -A/--all-namespaces and Deckhouse platform support on the server side.")
		// Shell completion for --scope values (enum + project:<name>). The
		// closure hands scopeCompletion the shared configFlags so project
		// names come from the same cluster the command itself would query.
		_ = getCmd.RegisterFlagCompletionFunc(scopeFlagName,
			func(_ *cobra.Command, _ []string, toComplete string) ([]string, cobra.ShellCompDirective) {
				return scopeCompletion(configFlags, toComplete)
			})
	}

	if debugCmd != nil {
		if imageFlag := debugCmd.Flags().Lookup("image"); imageFlag != nil {
			imageFlag.Usage = "Container image to use for debug container. If not specified, the platform's recommended image will be used."
		}
	}

	originalPersistentPreRunE := kubectlCmd.PersistentPreRunE
	kubectlCmd.PersistentPreRunE = func(cmd *cobra.Command, args []string) error {
		// Restore default OS signal handling for the kubectl subtree.
		//
		// The d8 root command installs a graceful-termination signal handler
		// (see graceful.WithTermination in NewDeliveryCommand) that intercepts
		// SIGINT/SIGTERM, cancels the root context and then resets the signal
		// handlers. The kubectl subcommands (notably long-running ones such as
		// `proxy`, `port-forward`, `exec`, `attach`, `logs -f`) do not observe
		// that context and keep running until a second signal arrives and is
		// delivered to the default Go handler.
		//
		// To match standalone kubectl behavior (single SIGINT/SIGTERM stops the
		// command), drop our signal interceptor before kubectl starts so the
		// very first signal is delivered to the default handler and terminates
		// the process immediately.
		signal.Reset(syscall.SIGINT, syscall.SIGTERM)

		// Reset the hint state on every invocation so a command tree reused
		// in-process (tests, embedding) cannot carry a previous run's arming
		// into an unrelated fatal message.
		scopeHintEligible, scopeHintResource = false, ""

		if scopeFlagValue != "" {
			// Validate the VALUE first: `--scope=bogus` without -A must be
			// reported as an invalid value, not as "requires -A" -- the
			// latter would just send the user into a second error after
			// they add -A.
			kind, project, err := parseScopeFlag(scopeFlagValue)
			if err != nil {
				return err
			}

			if err := scopeUsageError(cmd, scopeFlagValue); err != nil {
				return err
			}

			scopeKind, scopeProject = kind, project
		} else if cmd.Name() == "get" {
			// Arm the cluster-scope Forbidden hint: this invocation could have
			// used --scope but didn't (see scopeForbiddenHint). Check the flag
			// VALUE, not flag.Changed: --all-namespaces=false keeps the request
			// namespaced, so --scope would not have helped.
			if f := cmd.Flag("all-namespaces"); f != nil && f.Value.String() == "true" {
				scopeHintEligible = true

				if len(args) > 0 {
					scopeHintResource = args[0]
				}
			}
		}

		if cmd.Name() == "debug" || (cmd.Parent() != nil && cmd.Parent().Name() == "debug") {
			imageFlag := cmd.Flags().Lookup("image")
			if imageFlag != nil && imageFlag.Value.String() == "" {
				debugImage, err := getDebugImage(cmd)
				if err != nil {
					fmt.Fprintf(os.Stderr, "Warning: cannot get debug container image from cluster: %v\n", err)
					fmt.Fprintf(os.Stderr, "Continuing with default kubectl behavior...\n")
				} else {
					fmt.Fprintf(os.Stderr, "Using debug container image: %s\n", debugImage)

					if err := cmd.Flags().Set("image", debugImage); err != nil {
						fmt.Fprintf(os.Stderr, "Warning: cannot set debug image flag: %v\n", err)
					}
				}
			}
		}

		if originalPersistentPreRunE != nil {
			return originalPersistentPreRunE(cmd, args)
		}

		panic("originalPersistentPreRunE is nil, cannot proceed")
	}

	// Based on https://github.com/kubernetes/kubernetes/blob/v1.29.3/staging/src/k8s.io/component-base/cli/run.go#L88

	kubectlCmd.SetGlobalNormalizationFunc(cliflag.WordSepNormalizeFunc)
	kubectlCmd.SilenceErrors = true
	logs.AddFlags(kubectlCmd.PersistentFlags())

	switch {
	case kubectlCmd.PersistentPreRun != nil:
		pre := kubectlCmd.PersistentPreRun
		kubectlCmd.PersistentPreRun = func(cmd *cobra.Command, args []string) {
			logs.InitLogs()
			pre(cmd, args)
		}
	case kubectlCmd.PersistentPreRunE != nil:
		pre := kubectlCmd.PersistentPreRunE
		kubectlCmd.PersistentPreRunE = func(cmd *cobra.Command, args []string) error {
			logs.InitLogs()
			return pre(cmd, args)
		}
	default:
		kubectlCmd.PersistentPreRun = func(_ *cobra.Command, _ []string) {
			logs.InitLogs()
		}
	}

	return kubectlCmd
}
