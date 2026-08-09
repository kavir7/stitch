// Package web holds the dashboard. It is a single HTML file compiled into the
// binary, so the server has no runtime dependency on anything in this
// directory and there is no build step to run before starting it.
package web

import _ "embed"

//go:embed index.html
var Index []byte
