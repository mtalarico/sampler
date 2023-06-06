package cfg

import (
	"fmt"

	flag "github.com/spf13/pflag"

	"go.mongodb.org/mongo-driver/mongo/options"
)

type Compare struct {
	PrintWholeDoc bool
	Zscore        float64
	ErrorRate     float64
}

type MongoOptions struct {
	URI string
}

type Configuration struct {
	Source     MongoOptions
	Target     MongoOptions
	Meta       MongoOptions
	Compare    Compare
	MetaDBName string
	Verbosity  string
	LogFile    string
	CleanMeta  bool
	DryRun     bool
}

func Init() Configuration {
	config := Configuration{
		Source:  MongoOptions{},
		Target:  MongoOptions{},
		Meta:    MongoOptions{},
		Compare: Compare{},
	}

	flag.StringVar(&config.Source.URI, "sourceURI", "", "source connection string")
	flag.StringVar(&config.Target.URI, "targetURI", "", "target connection string")
	flag.StringVar(&config.Meta.URI, "metaURI", "", "meta connection string")
	flag.StringVar(&config.MetaDBName, "metaDBName", "sampler", "meta connection string")

	flag.BoolVar(&config.Compare.PrintWholeDoc, "printWholeDoc", false, fmt.Sprintf("%s\n\t- options: %s", "whether to print whole documents to the log (WARNING: can expose sensitive data)", "true, false (default false)"))
	flag.Float64Var(&config.Compare.Zscore, "zscore", 2.58, "float zscore associated with confidence level (assumes a normal distribution and random sampling)")
	flag.Float64Var(&config.Compare.ErrorRate, "errRate", 0.01, "error rate as a float percentage")

	flag.StringVar(&config.Verbosity, "verbosity", "info", fmt.Sprintf("%s\n\t- options: %s", "log level", "'error', 'warn', 'info', 'debug', 'trace'"))
	flag.StringVar(&config.LogFile, "filePath", "", "path to where log file should be stored. If not provided, no file is generated. The file name will be sampler-{datetime}.log for each run")
	flag.BoolVar(&config.CleanMeta, "cleanMeta", false, "drops metadata collection before reporting results")
	flag.BoolVar(&config.DryRun, "dryRun", false, "reports estimated counts + calculated sample size based on input z and error rate, then exits. Cannot be used in congunction with cleanMeta")

	flag.Parse()

	config.validate()

	return config
}

func (m *MongoOptions) MakeClientOptions() *options.ClientOptions {
	clientOps := options.Client().ApplyURI(m.URI).SetAppName("sampler")
	return clientOps
}

func (c *Configuration) validate() {
	if c.DryRun && c.CleanMeta {
		panic("cannot use cleanMeta with dryRun, specify one or the other")
	}
}
