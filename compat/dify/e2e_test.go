package dify

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"kairo/engine"
	"kairo/ir"
	"kairo/protocol"
)

// End to end (ADR 0028): a Dify workflow converted to kairo, run by the
// engine, its Dify nodes executed by the Python worker SDK running
// graphon's real node implementations (template-transform with Jinja2, an
// HTTP request to a local server). Needs GRAPHON_PYTHON, a Python with
// graphon==0.7.0; skipped otherwise.
func TestGraphonWorkerEndToEnd(t *testing.T) {
	py := os.Getenv("GRAPHON_PYTHON")
	if py == "" {
		t.Skip("GRAPHON_PYTHON not set")
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintf(w, `{"heard":%q}`, r.URL.Query().Get("greeting"))
	}))
	defer srv.Close()

	wf := `{"graph":{"nodes":[
	  {"id":"start","data":{"type":"start","title":"Start","variables":[{"variable":"name","type":"text-input"},{"variable":"url","type":"text-input"}]}},
	  {"id":"tpl","data":{"type":"template-transform","title":"Greet","template":"Hello {{ name }}!",
	    "variables":[{"variable":"name","value_selector":["start","name"],"value_type":"string"}]}},
	  {"id":"http","data":{"type":"http-request","title":"Call","method":"get","url":"{{#start.url#}}",
	    "authorization":{"type":"no-auth"},"headers":"","params":"greeting:{{#tpl.output#}}",
	    "body":{"type":"none","data":""},"timeout":{"connect":5,"read":5,"write":5}}},
	  {"id":"end","data":{"type":"end","title":"End","outputs":[
	    {"variable":"greeting","value_selector":["tpl","output"]},
	    {"variable":"status","value_selector":["http","status_code"]},
	    {"variable":"body","value_selector":["http","body"]}]}}],
	  "edges":[{"source":"start","target":"tpl"},{"source":"tpl","target":"http"},{"source":"http","target":"end"}]}}`
	var w Workflow
	if err := json.Unmarshal([]byte(wf), &w); err != nil {
		t.Fatal(err)
	}
	conv, err := Convert("e2e", &w)
	if err != nil {
		t.Fatal(err)
	}
	reg := ir.NewRegistry()
	for _, s := range conv.Specs {
		reg.Register(s)
	}
	e, err := engine.New(engine.Config{Shards: 1, Registry: reg})
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	if _, err := e.RegisterPlan(conv.Definition); err != nil {
		t.Fatal(err)
	}
	sock := filepath.Join(t.TempDir(), "w.sock")
	l, err := net.Listen("unix", sock)
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	go (&protocol.Server{E: e}).Serve(l)
	if err := e.Start(); err != nil {
		t.Fatal(err)
	}

	sdk, _ := filepath.Abs("../../sdk/python")
	cmd := exec.Command(py, "-m", "kairo_worker.serve", "--socket", sock,
		"--actions", "dify.template-transform,dify.http-request")
	cmd.Dir = sdk
	cmd.Env = append(os.Environ(), "PYTHONPATH="+sdk)
	cmd.Stdout, cmd.Stderr = os.Stdout, os.Stderr
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	defer cmd.Process.Kill()

	input, _ := json.Marshal(map[string]any{"name": "kairo", "url": srv.URL, "sys": map[string]any{"files": []any{}}})
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	res, err := e.Submit(ctx, engine.SubmitRequest{Plan: "e2e", Tenant: "t", Input: input})
	if err != nil {
		t.Fatal(err)
	}
	ri, err := e.Wait(ctx, res.RunID)
	if err != nil {
		t.Fatal(err)
	}
	var out struct {
		End struct {
			Greeting string `json:"greeting"`
			Status   int    `json:"status"`
			Body     string `json:"body"`
		} `json:"end"`
	}
	json.Unmarshal(ri.Output, &out)
	if ri.Status != "completed" || out.End.Greeting != "Hello kairo!" || out.End.Status != 200 || out.End.Body != `{"heard":"Hello kairo!"}` {
		t.Fatalf("%s %s %s", ri.Status, ri.Error, ri.Output)
	}
}
