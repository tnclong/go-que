package que

import (
	"runtime"
	"strconv"
	"strings"
)

// Stack reads and formats caller stack frames.
// The argument skip is the number of stack frames to skip.
func Stack(skip int) string {
	pcs := callers(skip)
	var b strings.Builder
	for _, pc := range pcs {
		pc = pc + 1
		fn := runtime.FuncForPC(pc)
		if fn == nil {
			b.WriteString("unknown")
		} else {
			file, line := fn.FileLine(pc)
			b.WriteString(fn.Name())
			b.WriteString("\n\t")
			b.WriteString(file)
			b.WriteByte(':')
			b.WriteString(strconv.FormatInt(int64(line), 10))
		}
		b.WriteByte('\n')
	}
	return b.String()
}

func callers(skip int) []uintptr {
	const deep = 32
	var pcs [deep]uintptr
	n := runtime.Callers(skip, pcs[:])
	return pcs[0:n]
}
