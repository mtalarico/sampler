package reporter

type DocSummary struct {
	MissingOnSrc int
	MissingOnTgt int
	Different    int
	Equal        int
}

func (ds DocSummary) HasMismatches() bool {
	return ds.MissingOnSrc > 0 || ds.MissingOnTgt > 0 || ds.Different > 0
}
