package reporter

type DocSummary struct {
	Missing   int
	Different int
	Equal     int
}

func (ds DocSummary) HasMismatches() bool {
	return ds.Missing > 0 || ds.Different > 0
}
