package output

import (
	"bytes"
	"context"
	"net/url"
	"os/exec"
	"strings"
	"time"

	"github.com/yuin/goldmark"
	"github.com/yuin/goldmark/ast"
	"github.com/yuin/goldmark/renderer"
	"github.com/yuin/goldmark/renderer/html"
	"github.com/yuin/goldmark/util"

	"shuttle/internal/gitenv"
	"shuttle/internal/provider/tkconfig"
)

const (
	// githubHost is on the allowlist unconditionally (§9.1).
	githubHost = "github.com"

	// remoteTimeout bounds the git call that reads the tk store's remote. It is
	// a local repository, so a slow one means a stalled mount.
	remoteTimeout = 10 * time.Second

	// mailtoScheme is allowed by both the link and the autolink path (§9.1).
	mailtoScheme = "mailto"

	// safePriority beats goldmark's default HTML renderer, which registers at
	// 1000: the lowest priority registers last and therefore wins.
	safePriority = 1
)

// Render turns report.md into the HTML §10 sends. Raw HTML is never emitted as
// markup and a link to a host outside allowHosts is rewritten to plain text
// with the URL in parentheses, gh-aw's redaction rule (§9.1).
func Render(markdown []byte, allowHosts []string) ([]byte, error) {
	md := goldmark.New(goldmark.WithRendererOptions(
		renderer.WithNodeRenderers(util.Prioritized(newSafeRenderer(allowHosts), safePriority)),
	))
	var buf bytes.Buffer
	if err := md.Convert(markdown, &buf); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// AllowHosts assembles §9.1's list: github.com, the host of the tk store's git
// remote and the configured domain list, lowercased and de-duplicated. A store
// with no remote, a missing tk config or a git that fails contributes no entry
// rather than failing the run.
func AllowHosts(extra []string, tkConfigPath string) []string {
	hosts := []string{githubHost}
	if h := storeRemoteHost(tkConfigPath); h != "" {
		hosts = append(hosts, h)
	}
	hosts = append(hosts, extra...)
	out := make([]string, 0, len(hosts))
	seen := map[string]bool{}
	for _, h := range hosts {
		h = strings.ToLower(strings.TrimSpace(h))
		if h == "" || seen[h] {
			continue
		}
		seen[h] = true
		out = append(out, h)
	}
	return out
}

func storeRemoteHost(tkConfigPath string) string {
	cfg, err := tkconfig.Load(tkConfigPath)
	if err != nil {
		return ""
	}
	ctx, cancel := context.WithTimeout(context.Background(), remoteTimeout)
	defer cancel()
	cmd := exec.CommandContext(ctx, "git", "-C", cfg.CentralRoot, "remote", "get-url", "origin")
	cmd.Env = gitenv.Env()
	out, err := cmd.Output()
	if err != nil {
		return ""
	}
	return remoteHost(strings.TrimSpace(string(out)))
}

// remoteHost pulls the host out of a git remote, which is either a URL or scp
// syntax (git@host:path). A local path has no host and contributes nothing.
func remoteHost(remote string) string {
	if u, err := url.Parse(remote); err == nil && u.Host != "" {
		return strings.ToLower(u.Hostname())
	}
	if _, rest, ok := strings.Cut(remote, "@"); ok {
		if host, _, ok := strings.Cut(rest, ":"); ok {
			return strings.ToLower(host)
		}
	}
	return ""
}

// safeRenderer replaces goldmark's raw-HTML and link rendering (§9.1).
// html.WithUnsafe is never set; the default renderer's substitute for raw HTML
// is an HTML comment, which loses the evidence, so the node's own source is
// written back escaped instead and a <script> or a tracking <img> arrives as
// text the operator can see.
type safeRenderer struct {
	hosts []string
	base  baseFuncs
}

// baseFuncs captures the stock HTML renderer's funcs so an allowed link or
// image is rendered exactly as goldmark would, escaping included.
type baseFuncs map[ast.NodeKind]renderer.NodeRendererFunc

func (b baseFuncs) Register(kind ast.NodeKind, f renderer.NodeRendererFunc) { b[kind] = f }

func newSafeRenderer(hosts []string) *safeRenderer {
	base := baseFuncs{}
	html.NewRenderer().RegisterFuncs(base)
	return &safeRenderer{hosts: hosts, base: base}
}

func (r *safeRenderer) RegisterFuncs(reg renderer.NodeRendererFuncRegisterer) {
	reg.Register(ast.KindRawHTML, r.renderRawHTML)
	reg.Register(ast.KindHTMLBlock, r.renderHTMLBlock)
	reg.Register(ast.KindLink, r.renderLink)
	reg.Register(ast.KindAutoLink, r.renderAutoLink)
	reg.Register(ast.KindImage, r.renderImage)
}

func (r *safeRenderer) renderRawHTML(w util.BufWriter, source []byte, node ast.Node, entering bool) (ast.WalkStatus, error) {
	if !entering {
		return ast.WalkSkipChildren, nil
	}
	n := node.(*ast.RawHTML)
	for i := 0; i < n.Segments.Len(); i++ {
		segment := n.Segments.At(i)
		_, _ = w.Write(util.EscapeHTML(segment.Value(source)))
	}
	return ast.WalkSkipChildren, nil
}

func (r *safeRenderer) renderHTMLBlock(w util.BufWriter, source []byte, node ast.Node, entering bool) (ast.WalkStatus, error) {
	n := node.(*ast.HTMLBlock)
	if entering {
		for i := 0; i < n.Lines().Len(); i++ {
			line := n.Lines().At(i)
			_, _ = w.Write(util.EscapeHTML(line.Value(source)))
		}
		return ast.WalkContinue, nil
	}
	if n.HasClosure() {
		_, _ = w.Write(util.EscapeHTML(n.ClosureLine.Value(source)))
	}
	return ast.WalkContinue, nil
}

func (r *safeRenderer) renderLink(w util.BufWriter, source []byte, node ast.Node, entering bool) (ast.WalkStatus, error) {
	n := node.(*ast.Link)
	dest := destination(n.Destination)
	if r.allowed(dest) {
		return r.base[ast.KindLink](w, source, node, entering)
	}
	// No tag at all: the children render as ordinary text between the entering
	// and exiting calls, and the URL follows in parentheses.
	if !entering {
		writeRedaction(w, dest)
	}
	return ast.WalkContinue, nil
}

func (r *safeRenderer) renderImage(w util.BufWriter, source []byte, node ast.Node, entering bool) (ast.WalkStatus, error) {
	n := node.(*ast.Image)
	dest := destination(n.Destination)
	if r.allowed(dest) {
		return r.base[ast.KindImage](w, source, node, entering)
	}
	// A markdown-syntax remote image is the same tracking pixel as the raw <img>
	// §9.1 names, so it is redacted to its alt text and the URL.
	if !entering {
		writeRedaction(w, dest)
	}
	return ast.WalkContinue, nil
}

// destination is the URL a link or image actually resolves to. goldmark keeps
// ast.Link.Destination as the raw markdown bytes and only decodes them while
// writing the href — util.URLEscape(dest, true), which unescapes backslashed
// punctuation and resolves character references. Deciding the allowlist on the
// raw bytes therefore checks a different host than the reader's browser sees:
// `https://evil.example&sol;.github.com/x` passes as a github.com subdomain and
// renders as `https://evil.example/.github.com/x` (§9.1). The same decoding is
// applied here, once, so the host checked is the host emitted; the base
// renderer decodes the raw destination once itself and lands on these bytes.
func destination(raw []byte) []byte {
	return util.URLEscape(raw, true)
}

func (r *safeRenderer) renderAutoLink(w util.BufWriter, source []byte, node ast.Node, entering bool) (ast.WalkStatus, error) {
	if !entering {
		return ast.WalkContinue, nil
	}
	n := node.(*ast.AutoLink)
	// Not run through destination(): goldmark renders an autolink's href with
	// util.URLEscape(url, false), so its source bytes are already the ones the
	// browser resolves.
	dest := n.URL(source)
	// URL() returns an email autolink's bare address while the base renderer
	// writes its href as mailto:<address>; the prefix is applied here so the
	// policy decides the bytes that reach the href, and <a@b.com> and
	// [x](mailto:a@b.com) therefore get the same answer (§9.1).
	if n.AutoLinkType == ast.AutoLinkEmail {
		dest = append([]byte(mailtoScheme+":"), dest...)
	}
	if r.allowed(dest) {
		return r.base[ast.KindAutoLink](w, source, node, entering)
	}
	// An autolink carries no children, so its label is written here.
	_, _ = w.Write(util.EscapeHTML(n.Label(source)))
	writeRedaction(w, dest)
	return ast.WalkContinue, nil
}

func writeRedaction(w util.BufWriter, dest []byte) {
	_, _ = w.WriteString(" (")
	_, _ = w.Write(util.EscapeHTML(dest))
	_, _ = w.WriteString(")")
}

// allowed applies §9.1's destination policy. A destination with neither scheme
// nor host is a relative link or a bare fragment and is not exfiltration.
// Everything that is not http or https on an allowed host — javascript:, data:,
// any other scheme — is redacted to text, so a javascript: URL can never reach
// an href.
func (r *safeRenderer) allowed(dest []byte) bool {
	u, err := url.Parse(string(dest))
	if err != nil {
		return false
	}
	if u.Scheme == "" && u.Host == "" && !schemeRelative(dest) {
		return true
	}
	// mailto is not the exfiltration channel the redaction rule exists for: a
	// mailto anchor opens a composer and fetches nothing when the message is
	// displayed, so both spellings of an address keep their anchor (§9.1).
	if u.Scheme == mailtoScheme {
		return true
	}
	if u.Scheme != "http" && u.Scheme != "https" {
		return false
	}
	host := strings.ToLower(u.Hostname())
	for _, h := range r.hosts {
		// A subdomain of an allowed host is allowed: www.github.com matches
		// github.com.
		if host == h || strings.HasSuffix(host, "."+h) {
			return true
		}
	}
	return false
}

// schemeRelative reports whether dest can be read as an authority where
// url.Parse reads it as a path. url.Parse gives ///attacker.example/x an empty
// Host, but the WHATWG URL parser ignores the run of slashes and resolves
// attacker.example as the host of a link in an HTTPS message body; it also
// treats a backslash as a slash for those schemes, and strips leading C0
// controls and space before parsing. The relative-link exemption is therefore
// decided on the raw shape after that same strip, over both slash characters:
// it is for a destination that cannot be read as scheme-relative at all, not
// for the shapes url.Parse happens to model (§9.1). goldmark percent-encodes a
// literal backslash on its way into an href, so the backslash forms reach here
// only from a path that hands over raw bytes.
func schemeRelative(dest []byte) bool {
	d := bytes.TrimLeftFunc(dest, func(r rune) bool { return r <= ' ' })
	return len(d) >= 2 && isSlash(d[0]) && isSlash(d[1])
}

func isSlash(c byte) bool { return c == '/' || c == '\\' }
