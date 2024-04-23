package reporter

type Location string
type Reason string

const (
	Source Location = "src"
	Target Location = "tgt"
)

const (
	COLL_SUMMARY Reason = "collSampleSummary"

	NS_MISSING    Reason = "namespaceMissing"
	INDEX_MISSING Reason = "indexMissing"
	DOC_MISSING   Reason = "docMissing"

	NS_DIFF    Reason = "namespaceMismatch"
	COUNT_DIFF Reason = "countMismatch"
	INDEX_DIFF Reason = "indexMismatch"
	DOC_DIFF   Reason = "docMismatch"
)

const NUM_REPORTERS uint = 1
