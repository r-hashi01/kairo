package n8n

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"strings"
	"time"
)

// Tokens between n8n's control plane and its data plane (engine v2): JWTs
// signed with HS256 and a shared secret of at least 32 characters, valid
// for 60 seconds, with 30 seconds of clock skew allowed.
//
//   - control plane -> data plane: iss "n8n-cp", aud "n8n-engine-dp"
//   - data plane -> control plane: iss "n8n-engine-dp", aud "n8n-cp", and a
//     scope ("lifecycle-events:write", "credentials:read")
const (
	IssuerCP    = "n8n-cp"
	IssuerDP    = "n8n-engine-dp"
	tokenTTL    = 60 * time.Second
	clockSkew   = 30 * time.Second
	MinSecret   = 32
	ScopeEvents = "lifecycle-events:write"
)

var errToken = errors.New("n8n: invalid token")

var b64 = base64.RawURLEncoding

// jwtHeader is the only header accepted: HS256, nothing else.
const jwtHeader = `{"alg":"HS256","typ":"JWT"}`

func sign(secret []byte, claims any) (string, error) {
	body, err := json.Marshal(claims)
	if err != nil {
		return "", err
	}
	unsigned := b64.EncodeToString([]byte(jwtHeader)) + "." + b64.EncodeToString(body)
	mac := hmac.New(sha256.New, secret)
	mac.Write([]byte(unsigned))
	return unsigned + "." + b64.EncodeToString(mac.Sum(nil)), nil
}

type claims struct {
	Iss    string `json:"iss"`
	Aud    any    `json:"aud"`
	Sub    string `json:"sub,omitempty"`
	Tenant string `json:"tenant_id,omitempty"`
	Scope  string `json:"scope,omitempty"`
	Iat    int64  `json:"iat"`
	Exp    int64  `json:"exp"`
}

// VerifyCP checks a token the control plane sent (Authorization: Bearer).
func VerifyCP(secret []byte, token string, now time.Time) error {
	parts := strings.Split(token, ".")
	if len(parts) != 3 {
		return errToken
	}
	hdr, err := b64.DecodeString(parts[0])
	if err != nil {
		return errToken
	}
	var h struct {
		Alg string `json:"alg"`
	}
	if json.Unmarshal(hdr, &h) != nil || h.Alg != "HS256" {
		return errToken
	}
	sig, err := b64.DecodeString(parts[2])
	if err != nil {
		return errToken
	}
	mac := hmac.New(sha256.New, secret)
	mac.Write([]byte(parts[0] + "." + parts[1]))
	if !hmac.Equal(sig, mac.Sum(nil)) {
		return errToken
	}
	body, err := b64.DecodeString(parts[1])
	if err != nil {
		return errToken
	}
	var c claims
	if json.Unmarshal(body, &c) != nil || c.Iss != IssuerCP || !hasAudience(c.Aud, IssuerDP) || c.Sub == "" {
		return errToken
	}
	iat, exp := time.Unix(c.Iat, 0), time.Unix(c.Exp, 0)
	if now.After(exp.Add(clockSkew)) || now.Before(iat.Add(-clockSkew)) || now.Sub(iat) > tokenTTL+clockSkew {
		return errToken
	}
	return nil
}

func hasAudience(aud any, want string) bool {
	switch a := aud.(type) {
	case string:
		return a == want
	case []any:
		for _, x := range a {
			if x == want {
				return true
			}
		}
	}
	return false
}

// ActionToken mints a token for a call to the control plane.
func ActionToken(secret []byte, scope string, now time.Time) (string, error) {
	return sign(secret, claims{Iss: IssuerDP, Aud: IssuerCP, Scope: scope, Iat: now.Unix(), Exp: now.Add(tokenTTL).Unix()})
}

// cpToken mints a control-plane token (tests and tools).
func cpToken(secret []byte, sub string, now time.Time) (string, error) {
	return sign(secret, claims{Iss: IssuerCP, Aud: IssuerDP, Sub: sub, Tenant: sub, Iat: now.Unix(), Exp: now.Add(tokenTTL).Unix()})
}
