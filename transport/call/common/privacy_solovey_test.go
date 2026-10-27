package common

import (
	"errors"
	"fmt"
	"net/url"
	"strings"
	"testing"
)

func TestCallDiagnosticsKeepPrivateResponseAndRequestMaterialOut(t *testing.T) {
	const secret = "private-room-and-access-token"
	err := fmt.Errorf("platform join: %w", &url.Error{Op: "Post", URL: "https://example.test/" + secret, Err: errors.New(secret)})
	for _, diagnostic := range []string{BodySnippet([]byte(secret)), MaskError(err)} {
		if diagnostic == "" || strings.Contains(diagnostic, secret) || strings.Contains(diagnostic, "https://") {
			t.Fatal("platform diagnostic exposed private material")
		}
	}
}
