// kairo-jev serves typed decisions with TypeSafe Jev (ADR 0055): to kairod
// as a worker, and to the SDKs' embedded runtime as actions over HTTP(S)
// (ADR 0052). Either or both:
//
//	kairo-jev -worker /var/lib/kairo/worker.sock -actions jev.noul,jev.choice.route
//	kairo-jev -http :8443 -tls-cert cert.pem -tls-key key.pem   (KAIRO_ACTION_SECRET)
//
// The Jev API key is TYPESAFE_API_KEY; JEV_MODEL and JEV_URL change the
// model and the endpoint.
package main

import (
	"context"
	"errors"
	"flag"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	jevapi "github.com/mattn/go-jev"

	"github.com/r-hashi01/kairo/executor/jev"
	"github.com/r-hashi01/kairo/httpaction"
	"github.com/r-hashi01/kairo/protocol"
)

func main() {
	worker := flag.String("worker", "", "kairod's worker socket: a unix socket path, or tcp:host:port")
	token := flag.String("token", os.Getenv("KAIRO_WORKER_TOKEN"), "kairod's worker token (ADR 0037)")
	actions := flag.String("actions", "jev.noul,jev.choice,jev.score", "the actions served to kairod (jev.choice.<name> per decision)")
	concurrency := flag.Int("concurrency", 16, "steps run at once (to kairod)")
	addr := flag.String("http", "", "serve actions over HTTP(S) at this address (POST .../action)")
	secret := flag.String("secret", os.Getenv("KAIRO_ACTION_SECRET"), "the secret the calls are signed with (ADR 0052)")
	cert := flag.String("tls-cert", "", "the certificate (PEM) to serve HTTPS with")
	key := flag.String("tls-key", "", "its key (PEM)")
	model := flag.String("model", os.Getenv("JEV_MODEL"), "the Jev model (default: the SDK's)")
	url := flag.String("url", os.Getenv("JEV_URL"), "the Jev endpoint (default: the SDK's)")
	flag.Parse()
	if *worker == "" && *addr == "" {
		log.Fatal("give -worker, -http, or both")
	}

	opts := []jevapi.ClientOption{jevapi.WithAPIKey(os.Getenv("TYPESAFE_API_KEY"))}
	if *model != "" {
		opts = append(opts, jevapi.WithModel(*model))
	}
	if *url != "" {
		opts = append(opts, jevapi.WithURL(jevapi.Endpoint(*url)))
	}
	x := jev.New(jevapi.NewClient(opts...))

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	var wg sync.WaitGroup

	if *worker != "" {
		network, address := "unix", *worker
		if a, ok := strings.CutPrefix(*worker, "tcp:"); ok {
			network, address = "tcp", a
		}
		wk := &protocol.Worker{Name: "kairo-jev", Actions: strings.Split(*actions, ","), Concurrency: *concurrency,
			Handler: x.Execute, Token: *token}
		wg.Go(func() {
			// Serve until stopped; when the connection breaks, connect again.
			for ctx.Err() == nil {
				err := wk.Run(ctx, network, address)
				if ctx.Err() != nil {
					return
				}
				log.Printf("kairod (%s): %v; connecting again", *worker, err)
				select {
				case <-time.After(time.Second):
				case <-ctx.Done():
				}
			}
		})
	}

	if *addr != "" {
		if *secret == "" {
			log.Fatal("-http needs the secret (-secret or KAIRO_ACTION_SECRET)")
		}
		srv := &http.Server{Addr: *addr, Handler: httpaction.Handler(*secret, x.Execute), ReadHeaderTimeout: 10 * time.Second}
		wg.Go(func() {
			var err error
			if *cert != "" {
				err = srv.ListenAndServeTLS(*cert, *key)
			} else {
				log.Printf("serving plain http at %s: callers allow it only on this machine or with allowInsecure", *addr)
				err = srv.ListenAndServe()
			}
			if err != nil && !errors.Is(err, http.ErrServerClosed) {
				log.Fatal(err)
			}
		})
		go func() {
			<-ctx.Done()
			shutdown, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			srv.Shutdown(shutdown)
		}()
	}
	wg.Wait()
}
