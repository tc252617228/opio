package opio

func MakeEmptyBinary() []byte {
	return []byte{mpBin8, 0}
}

func IsEmptyBinary(src []byte) bool {
	srcLen := len(src)
	if srcLen < 2 {
		return true
	}
	if mpBin8 == src[0] && 0 == src[1] {
		return true
	}
	return false
}
