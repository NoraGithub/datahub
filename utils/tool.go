package utils

import "fmt"

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
