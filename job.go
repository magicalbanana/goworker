package goworker

// Job is interface for jobs
type Job interface {
	Perform()
}
