package api

import (
	"net/http"
	"os"
	"syscall"
	"time"
)

// crash SIGKILLs the process. It exists so the demo can prove the durability
// claim rather than assert it, and it is only routed when the operator passes
// --unsafe-demo.
//
// SIGKILL specifically: a graceful shutdown would flush and close the log,
// which is exactly the thing the test is not allowed to rely on. The process
// has to die with whatever is in the page cache still in the page cache.
func (s *Server) crash(w http.ResponseWriter, r *http.Request) {
	s.logger.Printf("api: /debug/crash requested, sending SIGKILL to pid %d", os.Getpid())
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	w.Write([]byte(`{"status":"killing"}` + "\n"))
	if f, ok := w.(http.Flusher); ok {
		f.Flush()
	}

	// Long enough for the response to leave the socket, short enough that the
	// caller sees the death as immediate.
	go func() {
		time.Sleep(50 * time.Millisecond)
		syscall.Kill(os.Getpid(), syscall.SIGKILL)
	}()
}
