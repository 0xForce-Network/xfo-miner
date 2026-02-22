package process

import (
	"bufio"
	"io"
)

type LineHandler func(line string)

func ScanLines(reader io.Reader, handler LineHandler) error {
	if reader == nil || handler == nil {
		return nil
	}

	scanner := bufio.NewScanner(reader)
	buf := make([]byte, 0, 64*1024)
	scanner.Buffer(buf, 1024*1024)

	for scanner.Scan() {
		handler(scanner.Text())
	}

	return scanner.Err()
}
