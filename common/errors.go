package common

import (
	"errors"
)

var ErrLockAlreadyRequired = errors.New("锁被占用")
