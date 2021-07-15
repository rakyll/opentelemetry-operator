package mode

import (
	"log"

	lbdiscovery "github.com/otel-loadbalancer/discovery"
	"github.com/prometheus/common/model"
)

// TODO: Move config and discovery to this package.
// TODO: Rename this package. This component is not a load balancer but a sharder or an autoscaler.

/*
	Load balancer will serve on an HTTP server exposing /jobs/<job_id>/targets <- these are configured using least connection
	Load balancer will need information about the collectors in order to set the URLs
	Keep a Map of what each collector currently holds and update it based on new scrape target updates
*/

// Create a struct that holds collector - and jobs for that collector
// This struct will be parsed into endpoint with collector and jobs info

type Collector struct {
	Name       string
	NumTargets int
}

// TODO: Why do we have an _ in _link?

// Label to display on the http server
type LinkLabel struct {
	Link string `json:"_link"`
}

type CollectorJson struct {
	Link string                    `json:"_link"`
	Jobs []lbdiscovery.TargetGroup `json:"targets"`
}

type targetItem struct {
	JobName   string
	Link      LinkLabel
	TargetURL string
	Label     model.LabelSet
	Collector *Collector
}

// TODO: Remove cache, generate responses on the fly.
// This is a microoptimization, generating the responses is cheap.
type DisplayCache struct {
	DisplayJobs          map[string](map[string][]lbdiscovery.TargetGroup)
	DisplayCollectorJson map[string](map[string]CollectorJson)
	DisplayJobMapping    map[string]LinkLabel
	DisplayTargetMapping map[string][]lbdiscovery.TargetGroup
}

// LoadBalancer makes decisions to distribute work among
// a number of OpenTelemetry collectors based on the number of targets.
// Users need to call SetTargets when they have new targets in their
// clusters and call Refresh to process the new targets and reshard.
type LoadBalancer struct {
	Cache DisplayCache

	// TODO: guard with mutex where needed.
	targetsWaiting map[string]lbdiscovery.TargetData // temp buffer to keep targets that are waiting to be processed

	targets    map[string]lbdiscovery.TargetData // all current targets used by LoadBalancer to make decisions
	collectors map[string]*Collector             // all current collectors

	nextCollector *Collector
	targetItems   map[string]*targetItem // TODO: Merge this with targets, there should be one source of truth for all target state.
}

// findNextCollector finds the next collector with less number of targets.
func (lb *LoadBalancer) findNextCollector() {
	for _, v := range lb.collectors {
		if v.NumTargets < lb.nextCollector.NumTargets {
			lb.nextCollector = v
		}
	}
}

// SetTargets accepts the a list of targets that will be used to make
// load balancing decisions. This method should be called when where are
// new targets discovered or existing targets are shutdown.
func (lb *LoadBalancer) SetTargets(targets []lbdiscovery.TargetData) {
	// TODO: Guard lb.targetsWaiting.
	// Dump old data
	for k := range lb.targetsWaiting {
		delete(lb.targetsWaiting, k)
	}
	// Set new data
	for _, i := range targets {
		lb.targetsWaiting[i.JobName+i.Target] = i
	}
}

// SetCollectors sets the set of collectors with key=collectorName, value=Collector object.
func (lb *LoadBalancer) SetCollectors(collectors []string) {
	// TODO: Guard lb.collectors
	// TODO: How do we handle the new collectors?
	if len(collectors) == 0 {
		log.Fatal("no collector instances present")
	}
	for _, i := range collectors {
		collector := Collector{Name: i, NumTargets: 0}
		lb.collectors[i] = &collector
	}
	lb.nextCollector = lb.collectors[collectors[0]]
}

func (lb *LoadBalancer) generateCache() {
	// TODO: Remove.
	var compareMap = make(map[string][]targetItem) // CollectorName+jobName -> TargetItem
	for _, targetItem := range lb.targetItems {
		compareMap[targetItem.Collector.Name+targetItem.JobName] = append(compareMap[targetItem.Collector.Name+targetItem.JobName], *targetItem)
	}
	lb.Cache = DisplayCache{DisplayJobs: make(map[string]map[string][]lbdiscovery.TargetGroup), DisplayCollectorJson: make(map[string](map[string]CollectorJson))}
	for _, v := range lb.targetItems {
		lb.Cache.DisplayJobs[v.JobName] = make(map[string][]lbdiscovery.TargetGroup)
	}
	for _, v := range lb.targetItems {
		var jobsArr []targetItem
		jobsArr = append(jobsArr, compareMap[v.Collector.Name+v.JobName]...)

		var targetGroupList []lbdiscovery.TargetGroup
		targetItemSet := make(map[string][]targetItem)
		for _, m := range jobsArr {
			targetItemSet[m.JobName+m.Label.String()] = append(targetItemSet[m.JobName+m.Label.String()], m)
		}
		labelSet := make(map[string]model.LabelSet)
		for _, targetItemList := range targetItemSet {
			var targetArr []string
			for _, targetItem := range targetItemList {
				labelSet[targetItem.TargetURL] = targetItem.Label
				targetArr = append(targetArr, targetItem.TargetURL)
			}
			targetGroupList = append(targetGroupList, lbdiscovery.TargetGroup{Targets: targetArr, Labels: labelSet[targetArr[0]]})

		}
		lb.Cache.DisplayJobs[v.JobName][v.Collector.Name] = targetGroupList
	}
}

// updateCache gets called whenever Refresh gets called
func (lb *LoadBalancer) updateCache() {
	// TODO: Remove.

	lb.generateCache() // Create cached structure
	// Create the display maps
	lb.Cache.DisplayTargetMapping = make(map[string][]lbdiscovery.TargetGroup)
	lb.Cache.DisplayJobMapping = make(map[string]LinkLabel)
	for _, vv := range lb.targetItems {
		lb.Cache.DisplayCollectorJson[vv.JobName] = make(map[string]CollectorJson)
	}
	for k, v := range lb.Cache.DisplayJobs {
		for kk, vv := range v {
			lb.Cache.DisplayCollectorJson[k][kk] = CollectorJson{Link: "/jobs/" + k + "/targets" + "?collector_id=" + kk, Jobs: vv}
		}
	}
	for _, targetItem := range lb.targetItems {
		lb.Cache.DisplayJobMapping[targetItem.JobName] = LinkLabel{targetItem.Link.Link}
	}

	for k, v := range lb.Cache.DisplayJobs {
		for kk, vv := range v {
			lb.Cache.DisplayTargetMapping[k+kk] = vv
		}
	}

}

// Refesh needs to be called to process the new target updates.
// Until Refresh is called, old targets will be served.
func (lb *LoadBalancer) Refresh() {
	// TODO: Refresh needs to be safe for concurrent access.
	// Guard thte lb fields with a mutex where needed.
	lb.removeOutdatedTargets()
	lb.processWaitingTargets()
	lb.updateCache()
}

// removeOutdatedTargets removes targets that are no longer available.
func (lb *LoadBalancer) removeOutdatedTargets() {
	for k := range lb.targets {
		if _, ok := lb.targetsWaiting[k]; !ok {
			delete(lb.targets, k)
			lb.collectors[lb.targetItems[k].Collector.Name].NumTargets--
			delete(lb.targetItems, k)
		}
	}
}

// processWaitingTargets processes the newly set targets.
func (lb *LoadBalancer) processWaitingTargets() {
	for k, v := range lb.targetsWaiting {
		if _, ok := lb.targetItems[k]; !ok {
			lb.findNextCollector()
			lb.targets[k] = v
			targetItem := targetItem{
				JobName:   v.JobName,
				Link:      LinkLabel{"/jobs/" + v.JobName + "/targets"},
				TargetURL: v.Target,
				Label:     v.Labels,
				Collector: lb.nextCollector,
			}
			lb.nextCollector.NumTargets++
			lb.targetItems[v.JobName+v.Target] = &targetItem
		}
	}
}

func NewLoadBalancer() *LoadBalancer {
	return &LoadBalancer{
		targetsWaiting: make(map[string]lbdiscovery.TargetData),
		targets:        make(map[string]lbdiscovery.TargetData),
		collectors:     make(map[string]*Collector),
		targetItems:    make(map[string]*targetItem),
	}
}
