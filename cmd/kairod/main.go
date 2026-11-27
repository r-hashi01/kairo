// kairod runs the engine as a daemon, keeping its data in files. The
// kairod released (store/kairod) can keep it in a SQL database too
// (ADR 0087).
package main

import "github.com/r-hashi01/kairo/kairod"

func main() { kairod.Main(nil) }
