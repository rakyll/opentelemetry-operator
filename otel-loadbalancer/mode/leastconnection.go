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

type TargetItem struct {
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

type LoadBalancer struct {
	// TODO: don't export these fields and guard with mutex.
	TargetSet     map[string]lbdiscovery.TargetData
	TargetMap     map[string]lbdiscovery.TargetData
	CollectorMap  map[string]*Collector
	TargetItemMap map[string]*TargetItem
	Cache         DisplayCache
	NextCollector *Collector
}

// Basic implementation of least connection algorithm - can be enhance or replaced by another delegation algorithm
func (lb *LoadBalancer) setNextCollector() {
	for _, v := range lb.CollectorMap {
		if v.NumTargets < lb.NextCollector.NumTargets {
			lb.NextCollector = v
		}
	}
}

// Initlialize the set of targets which will be used to compare the targets in use by the collector instances
// This function will periodically be called when changes are made in the target discovery
func (lb *LoadBalancer) SetTargets(targetList []lbdiscovery.TargetData) {
	// Add mutex.

	// Dump old data
	for k := range lb.TargetSet {
		delete(lb.TargetSet, k)
	}

	// Set new data
	for _, i := range targetList {
		lb.TargetSet[i.JobName+i.Target] = i
	}
}

// SetCollectors sets the set of collectors with key=collectorName, value=Collector object.
func (lb *LoadBalancer) SetCollectors(collectors []string) {
	// TODO: mutex on this
	if len(collectors) == 0 {
		log.Fatal("no collector instances present")
	}

	for _, i := range collectors {
		collector := Collector{Name: i, NumTargets: 0}
		lb.CollectorMap[i] = &collector
	}
	lb.NextCollector = lb.CollectorMap[collectors[0]]
}

// Remove jobs from our struct that are no longer in the new set
func (lb *LoadBalancer) removeOutdatedTargets() {
	for k := range lb.TargetMap {
		if _, ok := lb.TargetSet[k]; !ok {
			delete(lb.TargetMap, k)
			lb.CollectorMap[lb.TargetItemMap[k].Collector.Name].NumTargets--
			delete(lb.TargetItemMap, k)
		}
	}
}

//Add jobs that were added into our struct
func (lb *LoadBalancer) addUpdatedTargets() {
	for k, v := range lb.TargetSet {
		if _, ok := lb.TargetItemMap[k]; !ok {
			lb.setNextCollector()
			lb.TargetMap[k] = v
			targetItem := TargetItem{
				JobName:   v.JobName,
				Link:      LinkLabel{"/jobs/" + v.JobName + "/targets"},
				TargetURL: v.Target,
				Label:     v.Labels,
				Collector: lb.NextCollector,
			}
			lb.NextCollector.NumTargets++
			lb.TargetItemMap[v.JobName+v.Target] = &targetItem
		}
	}
}

func (lb *LoadBalancer) generateCache() {
	// TODO: Remove.
	var compareMap = make(map[string][]TargetItem) // CollectorName+jobName -> TargetItem
	for _, targetItem := range lb.TargetItemMap {
		compareMap[targetItem.Collector.Name+targetItem.JobName] = append(compareMap[targetItem.Collector.Name+targetItem.JobName], *targetItem)
	}
	lb.Cache = DisplayCache{DisplayJobs: make(map[string]map[string][]lbdiscovery.TargetGroup), DisplayCollectorJson: make(map[string](map[string]CollectorJson))}
	for _, v := range lb.TargetItemMap {
		lb.Cache.DisplayJobs[v.JobName] = make(map[string][]lbdiscovery.TargetGroup)
	}
	for _, v := range lb.TargetItemMap {
		var jobsArr []TargetItem
		jobsArr = append(jobsArr, compareMap[v.Collector.Name+v.JobName]...)

		var targetGroupList []lbdiscovery.TargetGroup
		targetItemSet := make(map[string][]TargetItem)
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

// UpdateCache gets called whenever Refresh gets called
func (lb *LoadBalancer) updateCache() {
	// TODO: Remove.

	lb.generateCache() // Create cached structure
	// Create the display maps
	lb.Cache.DisplayTargetMapping = make(map[string][]lbdiscovery.TargetGroup)
	lb.Cache.DisplayJobMapping = make(map[string]LinkLabel)
	for _, vv := range lb.TargetItemMap {
		lb.Cache.DisplayCollectorJson[vv.JobName] = make(map[string]CollectorJson)
	}
	for k, v := range lb.Cache.DisplayJobs {
		for kk, vv := range v {
			lb.Cache.DisplayCollectorJson[k][kk] = CollectorJson{Link: "/jobs/" + k + "/targets" + "?collector_id=" + kk, Jobs: vv}
		}
	}
	for _, targetItem := range lb.TargetItemMap {
		lb.Cache.DisplayJobMapping[targetItem.JobName] = LinkLabel{targetItem.Link.Link}
	}

	for k, v := range lb.Cache.DisplayJobs {
		for kk, vv := range v {
			lb.Cache.DisplayTargetMapping[k+kk] = vv
		}
	}

}

// TODO: Add boolean flags to determine if any changes were made that should trigger Refresh
// Refresh is a function that is called periodically - this will create a cached structure to hold data for consistency
// when collectors perform GET operations
func (lb *LoadBalancer) Refresh() {
	// TODO: Refresh needs to be safe for concurrent access.
	lb.removeOutdatedTargets()
	lb.addUpdatedTargets()
	lb.updateCache()
}

// UpdateCache updates the DisplayMap so that mapping is consistent

func NewLoadBalancer() *LoadBalancer {
	return &LoadBalancer{
		TargetSet:     make(map[string]lbdiscovery.TargetData),
		TargetMap:     make(map[string]lbdiscovery.TargetData),
		CollectorMap:  make(map[string]*Collector),
		TargetItemMap: make(map[string]*TargetItem),
	}
}
