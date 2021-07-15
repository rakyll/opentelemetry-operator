package mode

import (
	"testing"

	lbdiscovery "github.com/otel-loadbalancer/discovery"
	"github.com/prometheus/common/model"
	"github.com/stretchr/testify/assert"
)

// Tests least connection - The expected collector after running SetNextCollector should be the collecter with the least amount of workload
func TestFindNextCollector(t *testing.T) {
	// prepare
	lb := NewLoadBalancer()
	defaultCol := Collector{Name: "default-col", NumTargets: 1}
	maxCol := Collector{Name: "max-col", NumTargets: 2}
	leastCol := Collector{Name: "least-col", NumTargets: 0}
	lb.collectors[maxCol.Name] = &maxCol
	lb.collectors[leastCol.Name] = &leastCol
	lb.nextCollector = &defaultCol

	// test
	lb.findNextCollector()

	// verify
	assert.Equal(t, "least-col", lb.nextCollector.Name)
}

func TestInitializingCollectors(t *testing.T) {
	// prepare
	cols := []string{"col-1", "col-2", "col-3"}
	lb := NewLoadBalancer()

	// test
	lb.SetCollectors(cols)

	// verify
	assert.Equal(t, len(cols), len(lb.collectors))
	for _, i := range cols {
		assert.NotNil(t, lb.collectors[i])
	}
}

func TestAddingAndRemovingTargetFlow(t *testing.T) {
	// prepare lb with initial targets and collectors
	lb := NewLoadBalancer()
	cols := []string{"col-1", "col-2", "col-3"}
	initTargets := []string{"targ:1000", "targ:1001", "targ:1002", "targ:1003", "targ:1004", "targ:1005"}
	lb.SetCollectors(cols)
	var targetList []lbdiscovery.TargetData
	for _, i := range initTargets {
		targetList = append(targetList, lbdiscovery.TargetData{JobName: "sample-name", Target: i, Labels: model.LabelSet{}})
	}

	// test that targets and collectors are added properly
	lb.SetTargets(targetList)
	lb.Refresh()

	// verify
	assert.True(t, len(lb.targets) == 6)
	assert.True(t, len(lb.targetItems) == 6)

	// prepare second round of targets
	tar := []string{"targ:1001", "targ:1002", "targ:1003", "targ:1004"}
	var tarL []lbdiscovery.TargetData
	for _, i := range tar {
		tarL = append(tarL, lbdiscovery.TargetData{JobName: "sample-name", Target: i, Labels: model.LabelSet{}})
	}

	// test that less targets are found - removed
	lb.SetTargets(tarL)
	lb.Refresh()

	// verify
	assert.True(t, len(lb.targets) == 4)
	assert.True(t, len(lb.targetItems) == 4)

	// verify results map
	for _, i := range tar {
		_, ok := lb.targets["sample-name"+i]
		assert.True(t, ok)
	}
}
