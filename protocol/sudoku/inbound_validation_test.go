package sudoku

import (
	"encoding/json"
	"testing"

	"github.com/sagernet/sing-box/log"
	"github.com/sagernet/sing-box/option"
)

func TestSudokuConstructionRejectsInvalidProtocolConfiguration(t *testing.T) {
	for name, fields := range map[string]map[string]any{
		"empty-key":         {"key": ""},
		"aead":              {"aead_method": "invalid-aead"},
		"padding-min":       {"padding_min": -1},
		"padding-max":       {"padding_max": 101},
		"padding-order":     {"padding_min": 80, "padding_max": 20},
		"handshake-timeout": {"handshake_timeout": -1},
		"mask-mode":         {"http_mask_mode": "invalid-mask"},
		"path-root":         {"path_root": "nested/path"},
	} {
		t.Run(name, func(t *testing.T) {
			fields["listen"], fields["listen_port"] = "127.0.0.1", 0
			if _, exists := fields["key"]; !exists {
				fields["key"] = "sudoku-constructor-fixture"
			}
			raw, _ := json.Marshal(fields)
			var options option.SudokuInboundOptions
			if err := json.Unmarshal(raw, &options); err != nil {
				t.Fatal(err)
			}
			candidate, err := NewInbound(t.Context(), nil, log.NewNOPFactory().Logger(), "invalid-sudoku", options)
			if err == nil || candidate != nil {
				t.Fatal("invalid protocol configuration acquired a runtime object")
			}
		})
	}
}

func TestSudokuConstructionPreservesZeroValuesAndServingKeyModes(t *testing.T) {
	for _, key := range []string{"sudoku-legacy-psk", "0100000000000000000000000000000000000000000000000000000000000000"} {
		for _, mode := range []string{"", "legacy", "stream", "poll", "auto", "ws"} {
			fields := map[string]any{"key": key, "listen": "127.0.0.1", "listen_port": 0, "aead_method": "none",
				"padding_min": 0, "padding_max": 0, "handshake_timeout": 0, "enable_pure_downlink": false, "http_mask_mode": mode}
			raw, _ := json.Marshal(fields)
			var options option.SudokuInboundOptions
			if err := json.Unmarshal(raw, &options); err != nil {
				t.Fatal(err)
			}
			candidate, err := NewInbound(t.Context(), nil, log.NewNOPFactory().Logger(), "valid-sudoku", options)
			if err != nil || candidate == nil {
				t.Fatal("valid protocol configuration rejected", err)
			}
			conf := candidate.(*Inbound).protoConf
			if conf.PaddingMin != 0 || conf.PaddingMax != 0 || conf.HandshakeTimeoutSeconds != 0 || conf.EnablePureDownlink {
				t.Fatal("constructor defaulted explicit zero/false values")
			}
		}
	}
}
