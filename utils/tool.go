package utils

import (
	"crypto/md5"
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"io"
)

func MaxLenString(strarray []string) (maxlen int, maxstr string) {
	for _, str := range strarray {
		if len(str) > maxlen {
			maxlen = len(str)
			maxstr = str
		}
	}
	return
}

func PrintFmt(a ...[]string) {
	var i, j int
	for i = 0; i < len(a[0]); i++ {
		//fmt.Println(len(a[i]))
		//fmt.Println(maxlen)
		for j = 0; j < len(a); j++ {
			maxlen, _ := MaxLenString(a[j])

			//fmt.Println(len(a))
			fmt.Print("   ")
			n, _ := fmt.Print(a[j][i])
			if n < maxlen {
				for k := 0; k < (maxlen + 5 - n); k++ {
					fmt.Print(" ")
				}
			} else {
				for k := 0; k < 5; k++ {
					fmt.Print(" ")
				}
			}
		}
		fmt.Print("\n")

	}

}

func getmd5string(s string) string {
	h := md5.New()
	h.Write([]byte(s))
	return hex.EncodeToString(h.Sum(nil))
}

func Getguid() string {
	b := make([]byte, 48)

	if _, err := io.ReadFull(rand.Reader, b); err != nil {
		return ""
	}
	return getmd5string(base64.URLEncoding.EncodeToString(b))
}
