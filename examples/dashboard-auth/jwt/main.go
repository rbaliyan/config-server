// Package jwt shows how to use dashboard.CookieAuth and dashboard.BearerTokenAuth
// with JWT validation.
//
// To run this example, copy the code block below into your own module and add:
//
//	go get github.com/golang-jwt/jwt/v5
package jwt

/*
Two patterns are shown:

 1. CookieAuth + JWT — the token arrives as an HttpOnly cookie set by an
    existing auth service. The middleware validates it; the browser sends the
    cookie automatically (credentials:"include"). No token-input UI is shown.

 2. BearerTokenAuth + JWT — the operator generates a short-lived token and
    pastes it into the dashboard's sidebar token field. Useful for headless
    or API-key workflows.

In both cases the SecurityGuard on the gRPC service should perform the same
JWT validation so that direct API calls are also protected.

---

	package main

	import (
		"crypto/rsa"
		"crypto/x509"
		"encoding/pem"
		"fmt"
		"log"
		"net/http"
		"os"

		jwtlib "github.com/golang-jwt/jwt/v5"

		"github.com/rbaliyan/config-server/dashboard"
		"github.com/rbaliyan/config-server/gateway"
	)

	func loadPublicKey(path string) (*rsa.PublicKey, error) {
		data, err := os.ReadFile(path)
		if err != nil {
			return nil, err
		}
		block, _ := pem.Decode(data)
		if block == nil {
			return nil, fmt.Errorf("no PEM block in %s", path)
		}
		key, err := x509.ParsePKIXPublicKey(block.Bytes)
		if err != nil {
			return nil, err
		}
		pub, ok := key.(*rsa.PublicKey)
		if !ok {
			return nil, fmt.Errorf("not an RSA public key")
		}
		return pub, nil
	}

	// jwtValidator returns a validate func for use with CookieAuth or
	// BearerTokenAuth. It checks RS256 signatures, issuer, and expiry.
	func jwtValidator(pub *rsa.PublicKey, issuer string) func(*http.Request, string) error {
		return func(_ *http.Request, tokenStr string) error {
			_, err := jwtlib.Parse(tokenStr,
				func(token *jwtlib.Token) (any, error) {
					if _, ok := token.Method.(*jwtlib.SigningMethodRSA); !ok {
						return nil, fmt.Errorf("unexpected signing method: %v", token.Header["alg"])
					}
					return pub, nil
				},
				jwtlib.WithIssuer(issuer),
				jwtlib.WithExpirationRequired(),
			)
			return err
		}
	}

	func main() {
		pub, err := loadPublicKey(os.Getenv("JWT_PUBLIC_KEY_PATH"))
		if err != nil {
			log.Fatal(err)
		}

		validate := jwtValidator(pub, "https://auth.myapp.com")

		// Pattern 1: JWT arrives as a cookie set by your auth service.
		// ClientConfig → {"auth-type":"cookie","auth-credentials":"include"}
		cookieAuth := dashboard.CookieAuth("auth-token", validate)

		// Pattern 2: Operator pastes a token into the dashboard sidebar.
		// ClientConfig → {"auth-type":"bearer","auth-header":"Authorization"}
		bearerAuth := dashboard.BearerTokenAuth(validate)

		// Choose based on deployment.
		var auth dashboard.DashboardAuth
		if os.Getenv("DASHBOARD_AUTH_MODE") == "bearer" {
			auth = bearerAuth
		} else {
			auth = cookieAuth
		}

		ctx := context.Background()
		handler, err := gateway.NewInProcessHandler(ctx, nil, // supply your svc
			gateway.WithDashboard(),
			gateway.WithDashboardAuth(auth),
		)
		if err != nil {
			log.Fatal(err)
		}
		defer handler.Close()
		log.Fatal(http.ListenAndServe(":8080", handler))
	}
*/
