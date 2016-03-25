package utils

import (
	"os"
	//"syscall"
)

func Mkdir(path string, perm ...os.FileMode) error {
	//mask := syscall.Umask(0)
	//defer syscall.Umask(mask)

	var permission os.FileMode = 0755

	if len(perm) > 0 {
		permission = perm[0]
	}

	return os.MkdirAll(path, permission)
}
