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

	flag.StringVar(&config.Source.URI, "src", "", "source connection string")
	flag.StringVar(&config.Target.URI, "dst", "", "target connection string")
	flag.StringVar(&config.Meta.URI, "meta", "", "meta connection string, defaults to target")
	flag.StringVar(&config.MetaDBName, "metadbname", "sampler", "meta connection string")

	// flag.BoolVar(&config.Compare.PrintWholeDoc, "printWholeDoc", false, fmt.Sprintf("%s\n\t- options: %s", "whether to print whole documents to the log (WARNING: can expose sensitive data)", "true, false (default false)"))
	flag.Float64Var(&config.Compare.Zscore, "zscore", 2.58, "*DONT TOUCH UNLESS YOU KNOW WHAT YOURE DOING* float zscore associated with confidence level (assumes a normal distribution and random sampling)")
	flag.Float64Var(&config.Compare.ErrorRate, "errRate", 0.01, "*DONT TOUCH UNLESS YOU KNOW WHAT YOURE DOING* error rate as a float percentage")

	flag.Int64Var(&config.Compare.ForceSampleSize, "forceSampleSize", 0, "override sampling logic and specify fixed number of docs to check")

	flag.StringVar(&config.Verbosity, "verbosity", "info", "log level [ error | warn | info | debug | trace ]")
	flag.StringVar(&config.LogFile, "log", "", "path to where log file should be stored. If not provided, no file is generated. The file name will be sampler-{datetime}.log for each run")
	flag.BoolVar(&config.CleanMeta, "clean", false, "drops metadata collection before reporting results")
	flag.BoolVar(&config.DryRun, "dry", false, "reports estimated counts + calculated sample size based on input z and error rate, then exits. Cannot be used in congunction with cleanMeta")

	flag.Usage = func() {
		flagSet := flag.CommandLine
		fmt.Printf("Usage of %s:\n", os.Args[0])
		required := []string{"src", "dst"}
		optional := []string{"meta", "metadbname", "verbosity", "log", "clean", "dry"}

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
		fmt.Println("missing required parameters: --dst")
		os.Exit(1)
	}
	if c.DryRun && c.CleanMeta {
		flag.Usage()
		fmt.Println("cannot use --clean with --dry, specify one or the other")
		os.Exit(1)
	}
}
