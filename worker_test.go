package goworker

import (
	"fmt"
	"runtime"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
)

// Job structure
type TestJob struct {
	SomeJobData   string
	SomeJobResult string
	Done          bool
}

// check interface implementation
var _ Job = (*TestJob)(nil)

func (j *TestJob) Perform() {
	j.SomeJobResult = j.SomeJobData
	j.Done = true
}

func TestGoWorker(t *testing.T) {
	// start dispatcher with 10 workers (goroutines) and jobsQueue channel size 20
	d := NewDispatcher(10, 20)

	allTestJobs := make([]*TestJob, 20)

	wg := &sync.WaitGroup{}
	wg.Add(1)

	go func() {
		defer func() {
			wg.Done()
		}()
		// add jobs to channel
		for i := 0; i < 20; i++ {
			tj := &TestJob{
				SomeJobData: "job number" + fmt.Sprintf("%d", i),
			}
			// put job to slice
			allTestJobs[i] = tj

			// add job to dispatcher
			d.AddJob(tj)
		}

		// check is jobs done

		for _, j := range allTestJobs {
			// waiting when job done and then assert
			for !j.Done {
				runtime.Gosched()
			}
			assert.Equal(t, j.SomeJobData, j.SomeJobResult)

			uj := new(TestJob)
			*uj = *j
			uj.Done = false
			// for testing unperformed jobs methods
			d.unperformedJobs = append(d.unperformedJobs, uj)
		}

		// count jobs
		assert.Equal(t, 0, d.CountJobs())

		// stop dispatcher
		d.Stop()

		// copy jobs
		uJ := d.GetUnperformedJobs()

		// assert len of jobs
		assert.Equal(t, 20, len(d.GetUnperformedJobs()))

		// clean jobs
		d.CleanUnperformedJobs()

		// assert len of jobs in dispatcher and in slice
		assert.Equal(t, 0, len(d.GetUnperformedJobs()))
		assert.Equal(t, 20, len(uJ))

		// add unperformed jobs again
		for i, job := range uJ {
			allTestJobs[i] = job.(*TestJob)

			// simulate highload to stop dispatcher when jobs queue not empty
			for i := 0; i < 1000; i++ {
				go d.AddJob(job)
			}
		}

		go d.Run()
		d.Stop()
	}()

	// start dispatcher
	d.Run()

	wg.Wait()
}
