package wireguard

// Adapted from Deposist cc14a046be0002aafd0950d06ea5c4aeb319f4e6 (GPLv3).

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestEndpointIPCBeforeInitialization(t *testing.T) {
	endpoint := new(Endpoint)

	_, err := endpoint.IpcGet()
	require.EqualError(t, err, "wireguard endpoint unavailable")
	require.EqualError(t, endpoint.IpcSet(""), "wireguard endpoint unavailable")
}
