// Package refsethttp carries refset dataset versions through HTTP, for
// read-your-writes across replicas.
//
// A writer that changed a dataset sends the new version to the client:
//
//	refsethttp.SetVersion(w, "plans", version) // Refset-Version: plans=51
//
// The client sends it back on its next requests (Refset-Min-Version:
// plans=51), and Middleware on whichever replica receives it puts it in the
// request context, where Dataset.GetCtx waits for that version:
//
//	mux.Handle("/reports", refsethttp.Middleware(handler))
//	plan, ok, err := plans.GetCtx(r.Context(), userID)
//
// Browsers can instead get the versions through cookies (Config.CookiePrefix),
// which they echo automatically. Browser JavaScript reading the response
// header across origins needs the header listed in
// Access-Control-Expose-Headers; this package does not set CORS headers.
//
// Malformed or oversized version headers are ignored, never an error response.
// With Config.SigningKey set, versions are signed and unsigned or forged
// values are ignored, so clients can't make a replica wait for versions that
// don't exist.
//
// The package uses only the standard library and refset.
package refsethttp

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"fmt"
	"maps"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/azbyluthfan/refset"
)

const (
	// DefaultVersionHeader is the response header writers set.
	DefaultVersionHeader = "Refset-Version"
	// DefaultMinVersionHeader is the request header clients send back.
	DefaultMinVersionHeader = "Refset-Min-Version"

	defaultCookieMaxAge = 10 * time.Minute

	// Limits on what the middleware accepts, so a client can't make it do
	// unbounded work.
	maxHeaderBytes  = 4096
	maxEntries      = 32
	maxDatasetBytes = 64
	minKeyBytes     = 16
	sigBytes        = 16 // truncated HMAC-SHA256
)

// Config configures a Handler. The zero value is valid and uses the default
// header names, no cookies and no signing.
type Config struct {
	// VersionHeader is the response header SetVersion writes.
	// Default "Refset-Version".
	VersionHeader string

	// MinVersionHeader is the request header Middleware reads.
	// Default "Refset-Min-Version".
	MinVersionHeader string

	// CookiePrefix, if set, makes SetVersion also set one cookie per dataset,
	// named CookiePrefix+dataset (for example "refset_plans"), and makes
	// Middleware read those cookies. Browsers then carry versions without
	// any client code.
	CookiePrefix string

	// CookieMaxAge is how long version cookies live. Read-your-writes only
	// needs them until replicas catch up, so short is fine. Default 10m.
	CookieMaxAge time.Duration

	// CookieInsecure drops the Secure attribute, for local development over
	// plain HTTP. Leave false in production.
	CookieInsecure bool

	// SigningKey, if set, signs every version SetVersion writes (HMAC-SHA256),
	// and Middleware ignores versions without a valid signature. At least 16
	// bytes; use a random secret shared by writers and readers.
	SigningKey []byte

	// VerifyKeys are extra keys Middleware accepts signatures from, for key
	// rotation. Writers always sign with SigningKey.
	VerifyKeys [][]byte
}

// Handler writes and reads version headers and cookies with one Config. The
// package-level functions use a Handler with the zero Config.
type Handler struct {
	versionHeader    string
	minVersionHeader string
	cookiePrefix     string
	cookieMaxAge     time.Duration
	cookieSecure     bool
	signKey          []byte
	verifyKeys       [][]byte
}

// New returns a Handler for cfg, or an error if cfg is invalid.
func New(cfg Config) (*Handler, error) {
	h := &Handler{
		versionHeader:    cfg.VersionHeader,
		minVersionHeader: cfg.MinVersionHeader,
		cookiePrefix:     cfg.CookiePrefix,
		cookieMaxAge:     cfg.CookieMaxAge,
		cookieSecure:     !cfg.CookieInsecure,
	}
	var errs []error
	if h.versionHeader == "" {
		h.versionHeader = DefaultVersionHeader
	}
	if h.minVersionHeader == "" {
		h.minVersionHeader = DefaultMinVersionHeader
	}
	for _, name := range []string{h.versionHeader, h.minVersionHeader} {
		if !isToken(name, 128) {
			errs = append(errs, fmt.Errorf("invalid header name %q", name))
		}
	}
	if h.cookiePrefix != "" && !isToken(h.cookiePrefix, 64) {
		errs = append(errs, fmt.Errorf("invalid cookie prefix %q", h.cookiePrefix))
	}
	switch {
	case h.cookieMaxAge == 0:
		h.cookieMaxAge = defaultCookieMaxAge
	case h.cookieMaxAge < time.Second:
		errs = append(errs, fmt.Errorf("CookieMaxAge must be at least 1s, got %v", h.cookieMaxAge))
	}
	if cfg.SigningKey != nil {
		if len(cfg.SigningKey) < minKeyBytes {
			errs = append(errs, fmt.Errorf("SigningKey must be at least %d bytes", minKeyBytes))
		}
		h.signKey = slices.Clone(cfg.SigningKey)
		h.verifyKeys = append(h.verifyKeys, h.signKey)
	}
	for _, k := range cfg.VerifyKeys {
		if len(k) < minKeyBytes {
			errs = append(errs, fmt.Errorf("VerifyKeys entries must be at least %d bytes", minKeyBytes))
		}
		h.verifyKeys = append(h.verifyKeys, slices.Clone(k))
	}
	if len(cfg.VerifyKeys) > 0 && cfg.SigningKey == nil {
		errs = append(errs, errors.New("VerifyKeys set without SigningKey"))
	}
	if len(errs) > 0 {
		return nil, fmt.Errorf("refsethttp: invalid config: %w", errors.Join(errs...))
	}
	return h, nil
}

var defaultHandler, _ = New(Config{})

// SetVersion records that the response's writer produced version v of
// dataset, using the default Config. See Handler.SetVersion.
func SetVersion(w http.ResponseWriter, dataset string, v uint64) {
	defaultHandler.SetVersion(w, dataset, v)
}

// SetVersions is SetVersion for several datasets, using the default Config.
func SetVersions(w http.ResponseWriter, versions map[string]uint64) {
	defaultHandler.SetVersions(w, versions)
}

// Middleware puts the request's minimum versions into its context, using the
// default Config. See Handler.Middleware.
func Middleware(next http.Handler) http.Handler { return defaultHandler.Middleware(next) }

// SetVersion records that the response's writer produced version v of
// dataset. It merges into the version header already on w, keeping the
// highest version per dataset, so it can be called once per dataset changed.
// With cookies enabled it also sets the dataset's cookie. Call it before
// writing the response status or body.
//
// It panics if dataset is not a valid name (1 to 64 characters of letters,
// digits, '.', '_' or '-'), which is a programming error: declare dataset
// names as constants shared with the Source.
func (h *Handler) SetVersion(w http.ResponseWriter, dataset string, v uint64) {
	h.SetVersions(w, map[string]uint64{dataset: v})
}

// SetVersions is SetVersion for several datasets at once.
func (h *Handler) SetVersions(w http.ResponseWriter, versions map[string]uint64) {
	for ds := range versions {
		if !validDataset(ds) {
			panic(fmt.Sprintf("refsethttp: invalid dataset name %q", ds))
		}
	}
	// Existing values were written by this handler, so they are trusted.
	merged, _ := h.parse(w.Header().Values(h.versionHeader), false)
	for ds, v := range versions {
		merged[ds] = max(merged[ds], v)
	}
	w.Header().Set(h.versionHeader, h.Format(merged))
	if h.cookiePrefix != "" {
		for ds := range versions {
			http.SetCookie(w, &http.Cookie{ //nolint:gosec // HttpOnly and SameSite=Lax always; Secure unless Config.CookieInsecure (local development)
				Name:     h.cookiePrefix + ds,
				Value:    h.formatValue(ds, merged[ds]),
				Path:     "/",
				MaxAge:   int(h.cookieMaxAge / time.Second),
				HttpOnly: true,
				Secure:   h.cookieSecure,
				SameSite: http.SameSiteLaxMode,
			})
		}
	}
}

// Middleware puts the minimum versions a request carries (header, and cookies
// if enabled) into its context with refset.ContextWithMinVersion, so
// Dataset.GetCtx waits for them. Malformed, oversized or (with signing)
// unsigned values are ignored; the request always proceeds.
func (h *Handler) Middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		versions, _ := h.MinVersions(r)
		if len(versions) == 0 {
			next.ServeHTTP(w, r)
			return
		}
		ctx := r.Context()
		for ds, v := range versions {
			ctx = refset.ContextWithMinVersion(ctx, ds, v)
		}
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

// MinVersions returns the valid minimum versions r carries, from the header
// and (if enabled) cookies, keeping the highest per dataset. The error
// describes anything that was ignored, for logging; it never needs to fail the
// request.
func (h *Handler) MinVersions(r *http.Request) (map[string]uint64, error) {
	versions, err := h.parse(r.Header.Values(h.minVersionHeader), true)
	if h.cookiePrefix == "" {
		return versions, err
	}
	errs := []error{err}
	for _, c := range r.Cookies() {
		ds, ok := strings.CutPrefix(c.Name, h.cookiePrefix)
		if !ok {
			continue
		}
		if len(versions) >= maxEntries {
			errs = append(errs, errors.New("too many version cookies"))
			break
		}
		v, perr := h.parseValue(ds, c.Value, true)
		if perr != nil {
			errs = append(errs, fmt.Errorf("cookie %s: %w", c.Name, perr))
			continue
		}
		versions[ds] = max(versions[ds], v)
	}
	return versions, errors.Join(errs...)
}

// Format renders versions as a header value, sorted by dataset name and
// signed if the handler has a signing key: "perms=9,plans=51".
func (h *Handler) Format(versions map[string]uint64) string {
	var b strings.Builder
	for i, ds := range slices.Sorted(maps.Keys(versions)) {
		if i > 0 {
			b.WriteByte(',')
		}
		b.WriteString(ds)
		b.WriteByte('=')
		b.WriteString(h.formatValue(ds, versions[ds]))
	}
	return b.String()
}

// Parse reads a version header value such as "plans=51, perms=9", verifying
// signatures if the handler has keys. Invalid entries are skipped and
// described in the error; valid ones are returned.
func (h *Handler) Parse(value string) (map[string]uint64, error) {
	return h.parse([]string{value}, true)
}

// ParseVersions is Parse with the default Config (no signing).
func ParseVersions(value string) (map[string]uint64, error) { return defaultHandler.Parse(value) }

func (h *Handler) parse(values []string, verify bool) (map[string]uint64, error) {
	out := make(map[string]uint64)
	total := 0
	for _, v := range values {
		total += len(v)
	}
	if total == 0 {
		return out, nil
	}
	if total > maxHeaderBytes {
		return out, fmt.Errorf("version header too long (%d bytes, limit %d); ignored", total, maxHeaderBytes)
	}
	var errs []error
	n := 0
	for _, value := range values {
		for entry := range strings.SplitSeq(value, ",") {
			entry = strings.TrimSpace(entry)
			if entry == "" {
				continue
			}
			if n++; n > maxEntries {
				errs = append(errs, fmt.Errorf("more than %d entries; rest ignored", maxEntries))
				return out, errors.Join(errs...)
			}
			ds, val, ok := strings.Cut(entry, "=")
			ds = strings.TrimSpace(ds)
			if !ok || !validDataset(ds) {
				errs = append(errs, fmt.Errorf("malformed entry %q", entry))
				continue
			}
			v, err := h.parseValue(ds, strings.TrimSpace(val), verify)
			if err != nil {
				errs = append(errs, fmt.Errorf("entry %q: %w", entry, err))
				continue
			}
			out[ds] = max(out[ds], v)
		}
	}
	return out, errors.Join(errs...)
}

// parseValue reads "51" or, when signing, "51~<signature>".
func (h *Handler) parseValue(ds, val string, verify bool) (uint64, error) {
	num, sig, signed := strings.Cut(val, "~")
	if num == "" || len(num) > 20 || strings.IndexFunc(num, func(r rune) bool { return r < '0' || r > '9' }) >= 0 {
		return 0, errors.New("version is not a decimal number")
	}
	v, err := strconv.ParseUint(num, 10, 64)
	if err != nil {
		return 0, errors.New("version out of range")
	}
	if !verify || len(h.verifyKeys) == 0 {
		return v, nil
	}
	if !signed {
		return 0, errors.New("unsigned version")
	}
	got, err := base64.RawURLEncoding.DecodeString(sig)
	if err != nil || len(got) != sigBytes {
		return 0, errors.New("malformed signature")
	}
	for _, k := range h.verifyKeys {
		if hmac.Equal(got, sign(k, ds, v)) {
			return v, nil
		}
	}
	return 0, errors.New("bad signature")
}

func (h *Handler) formatValue(ds string, v uint64) string {
	s := strconv.FormatUint(v, 10)
	if h.signKey == nil {
		return s
	}
	return s + "~" + base64.RawURLEncoding.EncodeToString(sign(h.signKey, ds, v))
}

// sign binds the signature to the dataset and version, so a signature can't
// be moved to another dataset or number.
func sign(key []byte, ds string, v uint64) []byte {
	m := hmac.New(sha256.New, key)
	m.Write([]byte("refset-version-v1\x00"))
	m.Write([]byte(ds))
	m.Write([]byte{0})
	m.Write([]byte(strconv.FormatUint(v, 10)))
	return m.Sum(nil)[:sigBytes]
}

func validDataset(s string) bool { return isToken(s, maxDatasetBytes) }

// isToken reports whether s is 1..limit bytes of letters, digits, '.', '_' or
// '-' (safe in header values, header names and cookie names).
func isToken(s string, limit int) bool {
	if s == "" || len(s) > limit {
		return false
	}
	for i := 0; i < len(s); i++ {
		if !isTokenByte(s[i]) {
			return false
		}
	}
	return true
}

func isTokenByte(c byte) bool {
	switch {
	case 'a' <= c && c <= 'z', 'A' <= c && c <= 'Z', '0' <= c && c <= '9':
		return true
	default:
		return c == '.' || c == '_' || c == '-'
	}
}
