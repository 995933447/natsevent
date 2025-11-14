package natsevent

import "fmt"

type LogfFunc func(format string, v ...any)

var LogErrorf LogfFunc = func(format string, v ...any) {
	fmt.Printf(format+"\n", v...)
}
