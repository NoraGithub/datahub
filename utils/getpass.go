package utils

import (
	//"fmt"
	//"github.com/asiainfoLDP/datahub/utils/terminal"
	"os"
	"syscall"
)

func getch() byte {
	/*if oldState, err := terminal.MakeRaw(0); err != nil {
		panic(err)
	} else {
		defer terminal.Restore(0, oldState)
	}

	var buf [1]byte
	if n, err := syscall.Read(0, buf[:]); n == 0 || err != nil {
		panic(err)
	}
	return buf[0]*/
	var buf [1]byte
	msvcrt, _ := syscall.LoadLibrary("msvcrt.dll")
	defer syscall.FreeLibrary(msvcrt)
	_getch, _ := syscall.GetProcAddress(msvcrt, "_getch")
	ch, _, err := syscall.Syscall(_getch, 0, 0, 0, 0)
	if err != 0 {
		return buf[0]
	}
	return byte(ch)
}

// getPasswd returns the input read from terminal.
// If masked is true, typing will be matched by asterisks on the screen.
// Otherwise, typing will echo nothing.
func getPasswd(masked bool) []byte {
	var pass, bs, mask []byte
	if masked {
		bs = []byte("\b \b")
		mask = []byte("*")
	}
	//fmt.Println("...")
	for {
		if v := getch(); v == 127 || v == 8 {
			if l := len(pass); l > 0 {
				pass = pass[:l-1]
				os.Stdout.Write(bs)
			}
		} else if v == 13 || v == 10 {
			break
		} else if v != 0 {
			pass = append(pass, v)
			os.Stdout.Write(mask)
		}
	}
	println()
	return pass
}

func GetPasswd(mask ...bool) []byte {
	//fmt.Println(mask, len(mask), mask[0])
	if len(mask) > 0 {
		return getPasswd(mask[0])
	}
	return getPasswd(false)
}

/*
// GetPasswd returns the password read from the terminal without echoing input.
// The returned byte array does not include end-of-line characters.
func GetPasswd() []byte {
	return getPasswd(false)
}

// GetPasswdMasked returns the password read from the terminal, echoing asterisks.
// The returned byte array does not include end-of-line characters.
func GetPasswdMasked() []byte {
	return getPasswd(true)
}

*/
