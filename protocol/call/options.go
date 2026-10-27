package call

import (
	"net/http"
	"strings"

	"github.com/sagernet/sing-box/option"
	E "github.com/sagernet/sing/common/exceptions"
)

// Validate settings before acquiring a dialer or starting platform workers. The
// buffer-limit fields have no implementation in the selected source lineage.
func validateCommonOptions(options option.CallCommonOptions, creator bool) error {
	switch options.Platform {
	case "telemost", "vk", "wbstream", "dion":
	case "bitrix":
		if creator {
			return E.New("call: bitrix does not implement a creator")
		}
	default:
		return E.New("call: unsupported platform")
	}
	if options.ReadBuffer < 0 {
		return E.New("call: read_buffer must be non-negative")
	}
	if options.MaxBufferedAmount != 0 || options.MemoryLimit != 0 {
		return E.New("call: max_buffered_amount and memory_limit are not implemented")
	}
	if options.Platform == "wbstream" || options.Platform == "bitrix" {
		switch options.Mode {
		case "", "dc", "video":
		default:
			return E.New("call: unsupported tunnel mode")
		}
	}
	return nil
}

func validateCredentials(cookies option.CallCookieList, joinLink string, creator bool) error {
	if strings.TrimSpace(joinLink) != joinLink || strings.ContainsAny(joinLink, "\r\n\x00") {
		return E.New("call: invalid join_link")
	}
	if !creator && joinLink == "" {
		return E.New("call: missing join_link")
	}
	if creator && len(cookies) == 0 {
		return E.New("call: creator cookies are required")
	}
	names := make(map[string]struct{}, len(cookies))
	for _, cookie := range cookies {
		if err := (&http.Cookie{Name: cookie.Name, Value: cookie.Value}).Valid(); err != nil {
			return E.New("call: invalid cookie entry")
		}
		if _, duplicate := names[cookie.Name]; duplicate {
			return E.New("call: duplicate cookie name")
		}
		names[cookie.Name] = struct{}{}
	}
	return nil
}
