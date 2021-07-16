package sharder

import (
	"encoding/json"
	"net/http"

	"github.com/gorilla/mux"
	"github.com/prometheus/common/model"
)

// TODO: Why do we have an _ in _link?
// TODO: Add missing JSON tags.

type linkJSON struct {
	Link string `json:"_link"`
}

type collectorJSON struct {
	Link string            `json:"_link"`
	Jobs []targetGroupJSON `json:"targets"`
}

type targetGroupJSON struct {
	Targets []string       `json:"targets"`
	Labels  model.LabelSet `json:"labels"`
}

type targetItem struct {
	JobName   string
	Link      linkJSON
	TargetURL string
	Label     model.LabelSet
	Collector *collector
}

// TODO: Consider removing cache, generate responses on the fly.
type displayCache struct {
	displayJobs          map[string](map[string][]targetGroupJSON)
	displayCollectorJson map[string](map[string]collectorJSON)
	displayJobMapping    map[string]linkJSON
	displayTargetMapping map[string][]targetGroupJSON
}

func (sharder *Sharder) JobHandler(w http.ResponseWriter, r *http.Request) {
	sharder.jsonHandler(w, r, sharder.cache.displayJobMapping)
}

func (sharder *Sharder) TargetsHandler(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()["collector_id"]
	params := mux.Vars(r)
	if len(q) == 0 {
		targets := sharder.cache.displayCollectorJson[params["job_id"]]
		sharder.jsonHandler(w, r, targets)
		return
	}
	data := sharder.cache.displayTargetMapping[params["job_id"]+q[0]]
	sharder.jsonHandler(w, r, data)
}

func (s *Sharder) jsonHandler(w http.ResponseWriter, r *http.Request, data interface{}) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(data)
}

func (sharder *Sharder) generateCache() {
	var compareMap = make(map[string][]targetItem) // CollectorName+jobName -> TargetItem
	for _, targetItem := range sharder.targetItems {
		compareMap[targetItem.Collector.Name+targetItem.JobName] = append(compareMap[targetItem.Collector.Name+targetItem.JobName], *targetItem)
	}
	sharder.cache = displayCache{displayJobs: make(map[string]map[string][]targetGroupJSON), displayCollectorJson: make(map[string](map[string]collectorJSON))}
	for _, v := range sharder.targetItems {
		sharder.cache.displayJobs[v.JobName] = make(map[string][]targetGroupJSON)
	}
	for _, v := range sharder.targetItems {
		var jobsArr []targetItem
		jobsArr = append(jobsArr, compareMap[v.Collector.Name+v.JobName]...)

		var targetGroupList []targetGroupJSON
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
			targetGroupList = append(targetGroupList, targetGroupJSON{Targets: targetArr, Labels: labelSet[targetArr[0]]})

		}
		sharder.cache.displayJobs[v.JobName][v.Collector.Name] = targetGroupList
	}
}

// updateCache gets called whenever Reshard gets called
func (sharder *Sharder) updateCache() {
	sharder.generateCache() // Create cached structure
	// Create the display maps
	sharder.cache.displayTargetMapping = make(map[string][]targetGroupJSON)
	sharder.cache.displayJobMapping = make(map[string]linkJSON)
	for _, vv := range sharder.targetItems {
		sharder.cache.displayCollectorJson[vv.JobName] = make(map[string]collectorJSON)
	}
	for k, v := range sharder.cache.displayJobs {
		for kk, vv := range v {
			sharder.cache.displayCollectorJson[k][kk] = collectorJSON{Link: "/jobs/" + k + "/targets" + "?collector_id=" + kk, Jobs: vv}
		}
	}
	for _, targetItem := range sharder.targetItems {
		sharder.cache.displayJobMapping[targetItem.JobName] = linkJSON{targetItem.Link.Link}
	}

	for k, v := range sharder.cache.displayJobs {
		for kk, vv := range v {
			sharder.cache.displayTargetMapping[k+kk] = vv
		}
	}
}
