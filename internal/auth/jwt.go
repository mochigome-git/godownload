package auth

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rsa"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"math/big"
	"net/http"
	"sync"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

type JWKS struct {
	Keys []JSONWebKey `json:"keys"`
}

type JSONWebKey struct {
	Kid string `json:"kid"`
	Kty string `json:"kty"`
	Alg string `json:"alg"`
	Use string `json:"use"`
	// RSA fields
	N string `json:"n"`
	E string `json:"e"`
	// EC fields
	Crv string `json:"crv"`
	X   string `json:"x"`
	Y   string `json:"y"`
}

type JWTVerifier struct {
	jwks      *JWKS
	jwksURL   string
	mu        sync.RWMutex
	lastFetch time.Time
	cacheTTL  time.Duration
}

func NewJWTVerifier(supabaseURL string) (*JWTVerifier, error) {
	v := &JWTVerifier{
		jwksURL:  supabaseURL + "/auth/v1/.well-known/jwks.json",
		cacheTTL: 1 * time.Hour,
	}

	if err := v.fetchJWKS(); err != nil {
		return nil, fmt.Errorf("failed to fetch JWKS: %w", err)
	}

	return v, nil
}

func (v *JWTVerifier) fetchJWKS() error {
	resp, err := http.Get(v.jwksURL)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	var jwks JWKS
	if err := json.NewDecoder(resp.Body).Decode(&jwks); err != nil {
		return err
	}

	v.mu.Lock()
	v.jwks = &jwks
	v.lastFetch = time.Now()
	v.mu.Unlock()

	return nil
}

func (v *JWTVerifier) getJWKS() (*JWKS, error) {
	v.mu.RLock()
	if v.jwks != nil && time.Since(v.lastFetch) < v.cacheTTL {
		defer v.mu.RUnlock()
		return v.jwks, nil
	}
	v.mu.RUnlock()

	if err := v.fetchJWKS(); err != nil {
		return nil, err
	}

	v.mu.RLock()
	defer v.mu.RUnlock()
	return v.jwks, nil
}

func (v *JWTVerifier) VerifyToken(tokenString string) (jwt.MapClaims, error) {
	jwks, err := v.getJWKS()
	if err != nil {
		return nil, fmt.Errorf("failed to get JWKS: %w", err)
	}

	token, err := jwt.Parse(tokenString, func(token *jwt.Token) (interface{}, error) {
		kid, ok := token.Header["kid"].(string)
		if !ok {
			return nil, fmt.Errorf("missing kid in token header")
		}

		for _, key := range jwks.Keys {
			if key.Kid == kid {
				switch token.Header["alg"] {
				case "RS256":
					return key.RSAPublicKey()
				case "ES256":
					return key.ECDSAPublicKey()
				default:
					return nil, fmt.Errorf("unsupported algorithm: %s", token.Header["alg"])
				}
			}
		}
		return nil, fmt.Errorf("key with kid %s not found", kid)
	})

	if err != nil || !token.Valid {
		return nil, fmt.Errorf("invalid token: %w", err)
	}

	claims, ok := token.Claims.(jwt.MapClaims)
	if !ok {
		return nil, fmt.Errorf("invalid claims type")
	}

	// Verify audience
	aud, ok := claims["aud"].(string)
	if !ok || aud != "authenticated" {
		return nil, fmt.Errorf("invalid audience: %v", claims["aud"])
	}

	return claims, nil
}

func (j *JSONWebKey) RSAPublicKey() (*rsa.PublicKey, error) {
	nBytes, err := base64.RawURLEncoding.DecodeString(j.N)
	if err != nil {
		return nil, fmt.Errorf("failed to decode N: %w", err)
	}

	eBytes, err := base64.RawURLEncoding.DecodeString(j.E)
	if err != nil {
		return nil, fmt.Errorf("failed to decode E: %w", err)
	}

	e := 0
	for _, b := range eBytes {
		e = e<<8 + int(b)
	}

	return &rsa.PublicKey{
		N: new(big.Int).SetBytes(nBytes),
		E: e,
	}, nil
}

func (j *JSONWebKey) ECDSAPublicKey() (*ecdsa.PublicKey, error) {
	if j.Kty != "EC" {
		return nil, fmt.Errorf("key type is not EC: %s", j.Kty)
	}

	var curve elliptic.Curve
	switch j.Crv {
	case "P-256":
		curve = elliptic.P256()
	case "P-384":
		curve = elliptic.P384()
	case "P-521":
		curve = elliptic.P521()
	default:
		return nil, fmt.Errorf("unsupported curve: %s", j.Crv)
	}

	xBytes, err := base64.RawURLEncoding.DecodeString(j.X)
	if err != nil {
		return nil, fmt.Errorf("failed to decode X: %w", err)
	}

	yBytes, err := base64.RawURLEncoding.DecodeString(j.Y)
	if err != nil {
		return nil, fmt.Errorf("failed to decode Y: %w", err)
	}

	return &ecdsa.PublicKey{
		Curve: curve,
		X:     new(big.Int).SetBytes(xBytes),
		Y:     new(big.Int).SetBytes(yBytes),
	}, nil
}
