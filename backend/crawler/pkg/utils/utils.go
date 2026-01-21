package utils

import (
	"errors"
	"net"
)

func IsRetryableNetworkError(err error) bool {
	var netErr net.Error

	return errors.As(err, &netErr)
}
