package models

import (
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	schedulingcron "github.com/openfoundry/openfoundry-go/libs/scheduling-cron"
)

func TestInferCronFlavorByFieldCount(t *testing.T) {
	t.Parallel()
	// 5 fields → Unix5.
	f, err := InferCronFlavor("*/5 * * * *")
	require.NoError(t, err)
	assert.Equal(t, schedulingcron.Unix5, f)
	// 6 fields → Quartz6.
	f, err = InferCronFlavor("0 */5 * * * *")
	require.NoError(t, err)
	assert.Equal(t, schedulingcron.Quartz6, f)
	// 4 and 7 fields are invalid.
	for _, expr := range []string{"* * * *", "* * * * * * *", ""} {
		_, err := InferCronFlavor(expr)
		require.Error(t, err, "expr=%q", expr)
	}
}

func TestParseScheduleCronLengthBound(t *testing.T) {
	t.Parallel()
	long := strings.Repeat("0 ", 100) + "0"
	require.Len(t, long, 201)
	_, err := ParseScheduleCron(long, "UTC")
	require.Error(t, err)
	var invalid *InvalidScheduleError
	require.ErrorAs(t, err, &invalid)
	assert.Equal(t, "cron", invalid.Field)
}

func TestResolveScheduleTimeZoneRejectsLocal(t *testing.T) {
	t.Parallel()
	_, err := ResolveScheduleTimeZone("Local")
	require.Error(t, err)
	var invalid *InvalidScheduleError
	require.ErrorAs(t, err, &invalid)
	assert.Equal(t, "time_zone", invalid.Field)
	assert.Contains(t, invalid.Reason, "Local")

	for _, name := range []string{"", "UTC", "America/New_York", "Asia/Shanghai"} {
		_, err := ResolveScheduleTimeZone(name)
		require.NoError(t, err, "tz=%q", name)
	}
	_, err = ResolveScheduleTimeZone("Not/AZone")
	require.Error(t, err)
}

func TestNormalizeScheduleSpecRules(t *testing.T) {
	t.Parallel()
	// Enabled requires a valid non-empty cron.
	_, err := NormalizeScheduleSpec("", "UTC", true)
	require.Error(t, err)
	// Empty cron + disabled is the legal clear state.
	spec, err := NormalizeScheduleSpec("", "", false)
	require.NoError(t, err)
	assert.Equal(t, "", spec.Cron)
	assert.Equal(t, "UTC", spec.TimeZone)
	// Valid unix5 while disabled validates too (explicit new input).
	spec, err = NormalizeScheduleSpec("*/5 * * * *", "UTC", false)
	require.NoError(t, err)
	assert.False(t, spec.Enabled)
	// Valid quartz6.
	_, err = NormalizeScheduleSpec("0 */5 * * * *", "UTC", true)
	require.NoError(t, err)
	// Invalid expression is refused with the typed error even when disabled.
	_, err = NormalizeScheduleSpec("*/5 * * *", "UTC", false)
	require.Error(t, err)
	var invalid *InvalidScheduleError
	require.ErrorAs(t, err, &invalid)
	// Bad time zone refused.
	_, err = NormalizeScheduleSpec("*/5 * * * *", "Local", false)
	require.Error(t, err)
	require.ErrorAs(t, err, &invalid)
	assert.Equal(t, "time_zone", invalid.Field)
}

func TestNextScheduleFireStrictlyAfterAnchor(t *testing.T) {
	t.Parallel()
	anchor := time.Date(2026, 11, 14, 10, 30, 0, 123456789, time.UTC)
	next, err := NextScheduleFire("0 * * * *", "UTC", anchor)
	require.NoError(t, err)
	assert.True(t, next.After(anchor), "next must be strictly after the anchor")
	assert.Equal(t, 11, next.Hour())
	// A wall-clock zone anchors the same way.
	nextSH, err := NextScheduleFire("0 9 * * *", "Asia/Shanghai", anchor)
	require.NoError(t, err)
	assert.Equal(t, 9, nextSH.In(mustLoad(t, "Asia/Shanghai")).Hour())
}

func TestNextScheduleFireNoFutureOccurrence(t *testing.T) {
	t.Parallel()
	// February 30th never exists in any year.
	_, err := NextScheduleFire("0 0 30 2 *", "UTC", time.Now())
	require.Error(t, err)
}

func mustLoad(t *testing.T, name string) *time.Location {
	t.Helper()
	loc, err := time.LoadLocation(name)
	require.NoError(t, err)
	return loc
}

// Unix5ConstForTest keeps the flavor assertion independent of exporting the
// scheduling-cron type here.
func Unix5ConstForTest() any { return "unix5" }
