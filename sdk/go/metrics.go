package kairo

import opmetrics "github.com/r-hashi01/kairo/metrics"

type Metric = opmetrics.Measurement
type MetricLabels = opmetrics.Labels
type MetricSink = opmetrics.Sink
type MetricSinkFunc = opmetrics.SinkFunc

const (
	MetricRunStartUnconfirmed  = opmetrics.RunStartUnconfirmed
	MetricRunStartRetries      = opmetrics.RunStartRetries
	MetricRunStartConfirmation = opmetrics.RunStartConfirmation
	MetricDrain                = opmetrics.Drain
	MetricDrainDuration        = opmetrics.DrainDuration
	MetricDrainStartingTasks   = opmetrics.DrainStartingTasks
	MetricDrainStartingDrives  = opmetrics.DrainStartingDrives
)

type metricEmitter struct{ stream *opmetrics.Stream }

func newMetricEmitter(sink MetricSink) *metricEmitter {
	if sink == nil {
		return nil
	}
	return &metricEmitter{stream: opmetrics.NewStream(sink, 1024, 128)}
}

func (m *metricEmitter) emit(v Metric) {
	if m != nil {
		m.stream.Emit(v)
	}
}

func (m *metricEmitter) close() {
	if m != nil {
		m.stream.Close()
	}
}

func (m *metricEmitter) stats() (dropped, failed uint64) {
	if m == nil {
		return 0, 0
	}
	return m.stream.Dropped(), m.stream.Failed()
}
