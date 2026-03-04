package ipx

import (
	"testing"
)

func TestIsInternalIp(t *testing.T) {
	tests := []struct {
		ip       string
		expected bool
		wantErr  bool
	}{
		{"127.0.0.1", true, false},
		{"10.0.0.1", true, false},
		{"172.16.0.1", true, false},
		{"172.31.255.255", true, false},
		{"192.168.1.1", true, false},
		{"8.8.8.8", false, false},
		{"172.32.0.1", false, false},  // outside 172.16.0.0/12
		{"11.0.0.1", false, false},    // outside 10.0.0.0/8
		{"192.169.1.1", false, false}, // outside 192.168.0.0/16
		{"invalid-ip", false, true},
		{"::1", true, false}, // ipv6 loopback is internal
	}

	for _, tt := range tests {
		t.Run(tt.ip, func(t *testing.T) {
			got, err := IsInternalIp(tt.ip)
			if (err != nil) != tt.wantErr {
				t.Errorf("IsInternalIp() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.expected {
				t.Errorf("IsInternalIp() = %v, want %v", got, tt.expected)
			}
		})
	}
}

func TestGetInternalIp(t *testing.T) {
	ip, err := GetInternalIp()
	if err != nil {
		// It's possible the machine has no internal IP (e.g. only loopback or public),
		// but in most dev environments (like this one) there should be one.
		// We log it but don't fail strictly if the environment is weird,
		// unless we are sure.
		t.Logf("GetInternalIp() error: %v", err)
	} else {
		t.Logf("Internal IP: %s", ip)
		// Verify the returned IP is indeed internal
		isInternal, err := IsInternalIp(ip)
		if err != nil {
			t.Errorf("GetInternalIp returned invalid IP: %v", err)
		}
		if !isInternal {
			t.Errorf("GetInternalIp returned non-internal IP: %s", ip)
		}
	}
}
