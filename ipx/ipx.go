package ipx

import (
	"errors"
	"net"
)

// GetInternalIp 获取本机的内网IP
func GetInternalIp() (string, error) {
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		return "", err
	}
	for _, addr := range addrs {
		var ip net.IP
		switch v := addr.(type) {
		case *net.IPNet:
			ip = v.IP
		case *net.IPAddr:
			ip = v.IP
		}

		if ip == nil || ip.IsLoopback() {
			continue
		}

		// 只获取IPv4
		ip = ip.To4()
		if ip == nil {
			continue
		}

		isInternal, err := IsInternalIp(ip.String())
		if err != nil {
			continue
		}
		if isInternal {
			return ip.String(), nil
		}
	}
	return "", errors.New("no internal ip found")
}

// IsInternalIp 判断是否为内网IP
// 包含:
// 10.0.0.0/8
// 172.16.0.0/12
// 192.168.0.0/16
// 127.0.0.0/8 (Loopback)
func IsInternalIp(ipStr string) (bool, error) {
	ip := net.ParseIP(ipStr)
	if ip == nil {
		return false, errors.New("invalid ip format")
	}

	if ip.IsLoopback() {
		return true, nil
	}

	ip4 := ip.To4()
	if ip4 == nil {
		// 这里暂不处理IPv6的内网判断，如果需要支持IPv6 Unique Local Address (fc00::/7) 可以扩展
		return false, nil
	}

	// 10.0.0.0/8
	if ip4[0] == 10 {
		return true, nil
	}

	// 172.16.0.0/12
	if ip4[0] == 172 && ip4[1] >= 16 && ip4[1] <= 31 {
		return true, nil
	}

	// 192.168.0.0/16
	if ip4[0] == 192 && ip4[1] == 168 {
		return true, nil
	}

	return false, nil
}
