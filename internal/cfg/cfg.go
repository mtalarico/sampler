package cfg

import (
	"fmt"
	"os"

	flag "github.com/spf13/pflag"

	"go.mongodb.org/mongo-driver/mongo/options"
)

type Compare struct {
	// PrintWholeDoc bool
	Zscore          float64
	ErrorRate       float64
	ForceSampleSize int64
}

type MongoOptions struct {
	URI string
}

type Configuration struct {
	Source        MongoOptions
	Target        MongoOptions
	Meta          MongoOptions
	Compare       Compare
	MetaDBName    string
	IncludeNS     *[]string
	Verbosity     string
	LogFile       string
	Filter        string
	CleanMeta     bool
	ReportFullDoc bool
}

func Init() Configuration {
	config := Configuration{
		Source:  MongoOptions{},
		Target:  MongoOptions{},
		Meta:    MongoOptions{},
		Compare: Compare{},
	}

	flag.StringVar(&config.Source.URI, "src", "", "source connection string")
	flag.StringVar(&config.Target.URI, "tgt", "", "target connection string")
	flag.StringVar(&config.Meta.URI, "meta", "", "meta connection string, defaults to target")
	flag.StringVar(&config.MetaDBName, "metadbname", "sampler", "meta connection string")

	flag.Float64Var(&config.Compare.Zscore, "zscore", 2.58, "*DONT TOUCH UNLESS YOU KNOW WHAT YOURE DOING* zscore as a float for Cochran's sample size")
	flag.Float64Var(&config.Compare.ErrorRate, "errRate", 0.01, "*DONT TOUCH UNLESS YOU KNOW WHAT YOURE DOING* error rate as a float percentage for Cochran's sample size")

	flag.Int64Var(&config.Compare.ForceSampleSize, "forceSampleSize", 0, "override sampling logic and specify fixed number of docs to check")

	flag.StringVar(&config.Verbosity, "verbosity", "info", "log level [ error | warn | info | debug | trace ]")
	flag.StringVar(&config.LogFile, "log", "", "path where log file should be stored. If not provided, no file is generated. The file name will be sampler-{datetime}.log for each run")
	flag.StringVar(&config.Filter, "filter", "", "path to filter file containing a list of namespaces to extended JSON filter (e.x: { \"test.test\": { \"ts\": { \"$gt\": { \"$date\": ... } } } })")

	flag.BoolVar(&config.CleanMeta, "clean", false, "drops metadata collection before reporting results")
	flag.BoolVar(&config.ReportFullDoc, "fulldoc", false, "report the whole document in the metadata.docs collection, using this option will add time to the validator and use additional disk space + load on the destination")

	config.IncludeNS = flag.StringArray("ns", nil, "namespace to check, pass this flag multiple times to check multiple namespaces")

	flag.Usage = func() {
		flagSet := flag.CommandLine
		fmt.Printf("Usage of %s:\n", os.Args[0])
		required := []string{"src", "tgt"}
		optional := []string{"ns", "meta", "metadbname", "verbosity", "log", "filter", "clean"}

		fmt.Println("[ required ]")
		for _, name := range required {
			flag := flagSet.Lookup(name)
			fmt.Printf("  --%-14s%s\n", flag.Name, flag.Usage)
		}
		fmt.Println("[ optional ]")
		for _, name := range optional {
			flag := flagSet.Lookup(name)
			fmt.Printf("  --%-14s%s\n", flag.Name, flag.Usage)
		}
	}

	flag.Parse()

	config.validate()

	return config
}

func (m *MongoOptions) MakeClientOptions() *options.ClientOptions {
	clientOps := options.Client().ApplyURI(m.URI).SetAppName("sampler")
	return clientOps
}

func (c *Configuration) validate() {
	if c.Source.URI == "" {
		flag.Usage()
		fmt.Println("missing required parameters: --src")
		os.Exit(1)
	}
	if c.Target.URI == "" {
		flag.Usage()
		fmt.Println("missing required parameters: --tgt")
		os.Exit(1)
	}
}
