package escape

import "strings"

var (
	Codes = map[byte][]byte{
		',': []byte(`\,`),
		'"': []byte(`\"`),
		' ': []byte(`\ `),
		'=': []byte(`\=`),
	}

	codesStr = map[string]string{}
)

func init() {
	for k, v := range Codes {
		codesStr[string(k)] = string(v)
	}
}

// 将转义后的字符串还原
func UnescapeString(in string) string {
	if strings.IndexByte(in, '\\') == -1 {
		return in
	}

	for b, esc := range codesStr {
		in = strings.Replace(in, esc, b, -1)
	}
	return in
}

// 字符串转义
func String(in string) string {
	for b, esc := range codesStr {
		in = strings.Replace(in, b, esc, -1)
	}
	return in
}
