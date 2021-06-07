package goworker

// JobPerformer is interface for jobs
type JobPerformer interface {
	Perform()
}
