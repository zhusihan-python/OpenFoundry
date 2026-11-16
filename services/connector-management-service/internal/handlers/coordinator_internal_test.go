package handlers

// Test-only observability bridge (review round3 item 3): exported from the
// internal test package so the EXTERNAL handlers_test package can wait for a
// task to be genuinely QUEUED (not merely reserved — OutstandingAdmissions
// explicitly includes the reserve→dispatch window per the coordinator
// invariant comments). No production API is added or changed.

// CoordinatorQueuedTaskCountForTest reports the number of tasks currently
// sitting in the coordinator queue (reserved-but-not-dispatched tasks are
// NOT counted; tasks picked up by a worker are already drained from the
// channel).
func CoordinatorQueuedTaskCountForTest(c *RunCoordinator) int {
	if c == nil {
		return 0
	}
	c.gateMu.Lock()
	defer c.gateMu.Unlock()
	return len(c.tasks)
}
