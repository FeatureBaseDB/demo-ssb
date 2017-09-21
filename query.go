package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"
)

// arange generates an "arithmetic range" slice. Example:
// arange(10, 20, 2) -> [10, 12, 14, 16, 18]
func arange(start, stop, step int) []int {
	x := make([]int, 0, (stop-start)/step)
	for n := start; n < stop; n += step {
		x = append(x, n)
	}
	return x
}

// UnravelIndex generates an N-dimensional index from a 1-dimensional index.
func UnravelIndex(index1 int, dim []int) []int {
	// Used to "vectorize" arbitrarily deep for-loops, similar to numpy.unravel_index.
	// indexN[0] cycles the fastest, indexN[N-1] cycles the slowest.
	// Example:
	// A 3D array of dimensions (`dim`) = (5, 4, 3) has 5*4*3 = 60 elements,
	// which are indexed in 1D (`index1`) as [0:59],
	// but also in 3D (`indexN`) as [(0,0,0),(0,0,1),...].
	// For 1D index n, the 3D index (n2, n1, n0) is:
	// n0 = n % 5
	// n1 = (n/5) % 4
	// n2 = (n/(5*4)) % 3

	indexN := make([]int, len(dim))
	denom := 1
	for n := 0; n < len(dim); n++ {
		indexN[n] = (index1 / denom) % (dim[n])
		denom *= dim[n]
	}
	return indexN
}

type BenchmarkResult struct {
	Name        string  `json:"name"`
	Iterations  int     `json:"iterations"`
	Concurrency int     `json:"concurrency"`
	BatchSize   int     `json:"batchsize"`
	Seconds     float64 `json:"seconds"`
	ColumnCount uint64  `json:"columncount"`
	Timestamp   int32   `json:"timestamp"`
}

// QuerySet encapsulates a small amount of information necessary for
// generating a grouped query set.
type QuerySet struct {
	Name       string
	Format     string
	ArgSets    [][]int
	dim        int
	iterations int
	lengths    []int

	// need to maintain this stuff for sorting on both input and output fields
	// Results    []QueryResult
	// sortfunc
	// resultfunc
}

type QueryResult struct {
	raw     string
	inputs  []interface{}
	outputs []interface{}
}

func NewQuerySet(name, fmt string, argsets [][]int) QuerySet {
	qs := QuerySet{}
	qs.Name = name
	qs.Format = fmt
	qs.ArgSets = argsets
	qs.dim = len(argsets)

	iterations := 1
	lens := make([]int, len(argsets))
	for n := 0; n < len(argsets); n++ {
		iterations *= len(argsets[n])
		lens[n] = len(argsets[n])
	}

	qs.iterations = iterations
	qs.lengths = lens

	return qs
}

func (s *QuerySet) String() string {
	return fmt.Sprintf("%d queries of form:\n%s", s.iterations, s.Format)
}

// QueryN generates the Nth query of a QuerySet, as a raw query string
func (s *QuerySet) QueryN(n int) string {
	inds := UnravelIndex(n, s.lengths)
	args := make([]interface{}, s.dim)
	for k := 0; k < s.dim; k++ {
		args[k] = s.ArgSets[k][inds[k]]
	}
	return fmt.Sprintf(s.Format+"\n", args...)
}

// QueryResultN generates the Nth query of a QuerySet, as a QueryResult
func (s *QuerySet) QueryResultN(n int) QueryResult {
	qr := QueryResult{}
	inds := UnravelIndex(n, s.lengths)
	qr.inputs = make([]interface{}, s.dim)
	qr.outputs = make([]interface{}, 1)
	for k := 0; k < s.dim; k++ {
		qr.inputs[k] = s.ArgSets[k][inds[k]]
	}
	qr.raw = fmt.Sprintf(s.Format+"\n", qr.inputs...)
	return qr
}

// RunSumMultiBatch sends queries in a QuerySet to the cluster in a configurable combination of
// batchSize and concurrency. Examples:
// concurrency=1, batchSize=(iteration count) -> equivalent to RunSumBatch
// concurrency=N, batchSize=1                 -> equivalent to RunSumConcurrent(N)
// concurrency=N, batchSize=10                -> sends concurrent batches of 10 queries
func (s *Server) RunSumMultiBatch(qs QuerySet, concurrency, batchSize int) BenchmarkResult {
	queries := make(chan string)
	results := make(chan int)

	// Add queries to channel
	go func() {
		qBatch := ""
		batchCount := 0
		for n := 0; n < qs.iterations; n++ {
			qBatch += qs.QueryN(n)
			// for sorting need this:
			// qrs[n] = qs.QueryResultN(n)
			// qBatch += qrs[n].raw
			batchCount++
			if batchCount == batchSize {
				queries <- qBatch
				batchCount = 0
				qBatch = ""
			}
		}
		if qBatch != "" {
			queries <- qBatch
		}
		close(queries)
	}()

	// Start workers.
	var wg = &sync.WaitGroup{}
	start := time.Now()
	for n := 0; n < concurrency; n++ {
		wg.Add(1)
		go func() {
			s.runRawSumBatchQuery(queries, results, wg)
		}()
	}
	go func() {
		wg.Wait()
		close(results)
	}()
	// TODO sort

	// Write results to file.
	// TODO needs to write query inputs as well to be more meaningful.
	timestamp := int32(time.Now().Unix())
	fname := fmt.Sprintf("results/%v-%v.txt", qs.Name, timestamp)
	err := os.MkdirAll("results", 0700)
	if err != nil {
		fmt.Printf("creating results directory: %v\n", err)
	}
	f, err := os.Create(fname)
	if err != nil {
		fmt.Printf("creating results file: %v\n", err)
	} else {
		defer f.Close()
		nn := 0
		for res := range results {
			n, err := f.WriteString(fmt.Sprintf("%v\n", res))
			nn += n
			if err != nil {
				fmt.Printf("writing results file: %v\n", err)
				break
			}
		}
		fmt.Printf("wrote %d bytes to %v\n", nn, fname)
	}

	// Return result object.
	seconds := time.Now().Sub(start).Seconds()
	return BenchmarkResult{
		qs.Name,
		qs.iterations,
		concurrency,
		batchSize,
		seconds,
		s.NumLineOrders,
		timestamp,
	}
}

// runRawSumBatchQuery sends RawQueries to the cluster, then sends the Sum from each result to a result channel.
func (s *Server) runRawSumBatchQuery(queries <-chan string, results chan<- int, wg *sync.WaitGroup) {
	defer wg.Done()
	for q := range queries {
		response, err := s.Client.Query(s.Index.RawQuery(q), nil)
		if err != nil {
			fmt.Printf("in runRawSumBatchQuery: %vfailed with: %v\n", q, err)
			if strings.Contains(err.Error(), "invalid argument value") {
				fmt.Println("server may not support BETWEEN queries")
			}
			return
		}
		for _, res := range response.Results() {
			results <- int(res.Sum)
		}
	}
}

func (s *Server) HandleQuery(w http.ResponseWriter, r *http.Request) {
	qs := getQuerySet(r.URL.Path)
	var results []BenchmarkResult
	if s.concurrency > 0 {
		results = []BenchmarkResult{
			s.RunSumMultiBatch(qs, s.concurrency, s.batchSize),
		}
	} else {
		results = []BenchmarkResult{
			s.RunSumMultiBatch(qs, 1, 1),
			s.RunSumMultiBatch(qs, 1, 2),
			s.RunSumMultiBatch(qs, 1, 4),
			s.RunSumMultiBatch(qs, 1, 8),
			s.RunSumMultiBatch(qs, 1, 16),
			s.RunSumMultiBatch(qs, 2, 1),
			s.RunSumMultiBatch(qs, 2, 2),
			s.RunSumMultiBatch(qs, 2, 4),
			s.RunSumMultiBatch(qs, 2, 8),
			s.RunSumMultiBatch(qs, 2, 16),
			s.RunSumMultiBatch(qs, 4, 1),
			s.RunSumMultiBatch(qs, 4, 2),
			s.RunSumMultiBatch(qs, 4, 4),
			s.RunSumMultiBatch(qs, 4, 8),
			s.RunSumMultiBatch(qs, 4, 16),
			s.RunSumMultiBatch(qs, 8, 1),
			s.RunSumMultiBatch(qs, 8, 2),
			s.RunSumMultiBatch(qs, 8, 4),
			s.RunSumMultiBatch(qs, 8, 8),
			s.RunSumMultiBatch(qs, 8, 16),
			s.RunSumMultiBatch(qs, 16, 1),
			s.RunSumMultiBatch(qs, 16, 2),
			s.RunSumMultiBatch(qs, 16, 4),
			s.RunSumMultiBatch(qs, 16, 8),
			s.RunSumMultiBatch(qs, 16, 16),
			s.RunSumMultiBatch(qs, 32, 1),
			s.RunSumMultiBatch(qs, 32, 2),
			s.RunSumMultiBatch(qs, 32, 4),
			s.RunSumMultiBatch(qs, 32, 8),
			s.RunSumMultiBatch(qs, 32, 16),
		}
	}

	enc := json.NewEncoder(w)
	err := enc.Encode(results)
	if err != nil {
		fmt.Printf("writing results: %v to responsewriter: %v", results, err)
	}
}

func getQuerySet(q string) QuerySet {
	var qs QuerySet
	qname := strings.Split(q, "/")[2]
	fmt.Printf("defining QuerySet for %v (%v)\n", qname, q)
	switch qname {
	case "1.1":
		years := []int{1993}
		qs = NewQuerySet(
			"1.1",
			`Sum(
	Intersect(
		Bitmap(frame="lo_year", rowID=%d),
		Range(frame="lo_discount", lo_discount >= 1),
		Range(frame="lo_discount", lo_discount <= 3),
		Range(frame="lo_quantity", lo_quantity < 25)
	),
frame="lo_revenue_computed", field="lo_revenue_computed")`,
			[][]int{years},
		)

	case "1.2":
		years := []int{1994}
		qs = NewQuerySet(
			"1.2",
			`Sum(
	Intersect(
		Bitmap(frame="lo_month", rowID=0),
		Bitmap(frame="lo_year", rowID=%d),
		Range(frame="lo_discount", lo_discount >= 4),
		Range(frame="lo_discount", lo_discount <= 6),
		Range(frame="lo_quantity", lo_quantity >= 26),
		Range(frame="lo_quantity", lo_quantity <= 35)
	),
frame="lo_revenue_computed", field="lo_revenue_computed")`,
			[][]int{years},
		)

	case "1.3":
		years := []int{1994}
		qs = NewQuerySet(
			"1.3",
			`Sum(
	Intersect(
		Bitmap(frame="lo_weeknum", rowID=6),
		Bitmap(frame="lo_year", rowID=%d),
		Range(frame="lo_discount", lo_discount >= 5),
		Range(frame="lo_discount", lo_discount <= 7),
		Range(frame="lo_quantity", lo_quantity >= 26),
		Range(frame="lo_quantity", lo_quantity <= 35)
	),
frame="lo_revenue_computed", field="lo_revenue_computed")`,
			[][]int{years},
		)

	case "1.1b":
		years := []int{1993}
		qs = NewQuerySet(
			"1.1b",
			`Sum(
	Intersect(
		Bitmap(frame="lo_year", rowID=%d),
		Union(
			Bitmap(frame=lo_discount_b, rowID=1),
			Bitmap(frame=lo_discount_b, rowID=2),
			Bitmap(frame=lo_discount_b, rowID=3)),
		Union(
			Bitmap(frame=lo_quantity_b, rowID=1),
			Bitmap(frame=lo_quantity_b, rowID=2),
			Bitmap(frame=lo_quantity_b, rowID=3),
			Bitmap(frame=lo_quantity_b, rowID=4),
			Bitmap(frame=lo_quantity_b, rowID=5),
			Bitmap(frame=lo_quantity_b, rowID=6),
			Bitmap(frame=lo_quantity_b, rowID=7),
			Bitmap(frame=lo_quantity_b, rowID=8),
			Bitmap(frame=lo_quantity_b, rowID=9),
			Bitmap(frame=lo_quantity_b, rowID=10),
			Bitmap(frame=lo_quantity_b, rowID=11),
			Bitmap(frame=lo_quantity_b, rowID=12),
			Bitmap(frame=lo_quantity_b, rowID=13),
			Bitmap(frame=lo_quantity_b, rowID=14),
			Bitmap(frame=lo_quantity_b, rowID=15),
			Bitmap(frame=lo_quantity_b, rowID=16),
			Bitmap(frame=lo_quantity_b, rowID=17),
			Bitmap(frame=lo_quantity_b, rowID=18),
			Bitmap(frame=lo_quantity_b, rowID=19),
			Bitmap(frame=lo_quantity_b, rowID=20),
			Bitmap(frame=lo_quantity_b, rowID=21),
			Bitmap(frame=lo_quantity_b, rowID=22),
			Bitmap(frame=lo_quantity_b, rowID=23),
			Bitmap(frame=lo_quantity_b, rowID=24))
	),
frame="lo_revenue_computed", field="lo_revenue_computed")`,
			[][]int{years},
		)

	case "1.2b":
		years := []int{1994}
		qs = NewQuerySet(
			"1.2b",
			`Sum(
	Intersect(
		Bitmap(frame="lo_month", rowID=0),
		Bitmap(frame="lo_year", rowID=%d),
		Union(
			Bitmap(frame=lo_discount_b, rowID=4),
			Bitmap(frame=lo_discount_b, rowID=5),
			Bitmap(frame=lo_discount_b, rowID=6)),
		Union(
			Bitmap(frame=lo_quantity_b, rowID=26),
			Bitmap(frame=lo_quantity_b, rowID=27),
			Bitmap(frame=lo_quantity_b, rowID=28),
			Bitmap(frame=lo_quantity_b, rowID=29),
			Bitmap(frame=lo_quantity_b, rowID=30),
			Bitmap(frame=lo_quantity_b, rowID=31),
			Bitmap(frame=lo_quantity_b, rowID=32),
			Bitmap(frame=lo_quantity_b, rowID=33),
			Bitmap(frame=lo_quantity_b, rowID=34),
			Bitmap(frame=lo_quantity_b, rowID=35),
			Bitmap(frame=lo_quantity_b, rowID=36))
	),
frame="lo_revenue_computed", field="lo_revenue_computed")`,
			[][]int{years},
		)

	case "1.3b":
		years := []int{1994}
		qs = NewQuerySet(
			"1.3b",
			`Sum(
	Intersect(
		Bitmap(frame="lo_weeknum", rowID=6),
		Bitmap(frame="lo_year", rowID=%d),
		Union(
			Bitmap(frame=lo_discount_b, rowID=5),
			Bitmap(frame=lo_discount_b, rowID=6),
			Bitmap(frame=lo_discount_b, rowID=7)),
		Union(
			Bitmap(frame=lo_quantity_b, rowID=26),
			Bitmap(frame=lo_quantity_b, rowID=27),
			Bitmap(frame=lo_quantity_b, rowID=28),
			Bitmap(frame=lo_quantity_b, rowID=29),
			Bitmap(frame=lo_quantity_b, rowID=30),
			Bitmap(frame=lo_quantity_b, rowID=31),
			Bitmap(frame=lo_quantity_b, rowID=32),
			Bitmap(frame=lo_quantity_b, rowID=33),
			Bitmap(frame=lo_quantity_b, rowID=34),
			Bitmap(frame=lo_quantity_b, rowID=35),
			Bitmap(frame=lo_quantity_b, rowID=36))
	),
frame="lo_revenue_computed", field="lo_revenue_computed")`,
			[][]int{years},
		)

	case "1.1c":
		years := []int{1993}
		qs = NewQuerySet(
			"1.1c",
			`Sum(
	Intersect(
		Bitmap(frame="lo_year", rowID=%d),
		Range(frame="lo_discount", lo_discount >< [1,3]),
		Range(frame="lo_quantity", lo_quantity < 25)
	),
frame="lo_revenue_computed", field="lo_revenue_computed")`,
			[][]int{years},
		)

	case "1.2c":
		years := []int{1994}
		qs = NewQuerySet(
			"1.2c",
			`Sum(
	Intersect(
		Bitmap(frame="lo_month", rowID=0),
		Bitmap(frame="lo_year", rowID=%d),
		Range(frame="lo_discount", lo_discount >< [4,6]),
		Range(frame="lo_quantity", lo_quantity >< [26,35]),
	),
frame="lo_revenue_computed", field="lo_revenue_computed")`,
			[][]int{years},
		)

	case "1.3c":
		years := []int{1994}
		qs = NewQuerySet(
			"1.3c",
			`Sum(
	Intersect(
		Bitmap(frame="lo_weeknum", rowID=6),
		Bitmap(frame="lo_year", rowID=%d),
		Range(frame="lo_discount", lo_discount >< [5,7]),
		Range(frame="lo_quantity", lo_quantity >< [26,35]),
	),
frame="lo_revenue_computed", field="lo_revenue_computed")`,
			[][]int{years},
		)

	case "2.1":
		years := arange(1992, 1999, 1) // all years
		brands := arange(40, 80, 1)    // brands for the second manufacturer, "MFGR#12"
		// regionID := 0  // America
		qs = NewQuerySet(
			"2.1",
			`Sum(
	Intersect(
		Bitmap(frame="p_brand1", rowID=%d),
		Bitmap(frame="lo_year", rowID=%d),
		Bitmap(frame="s_region", rowID=0),
	),
	frame="lo_revenue", field="lo_revenue")`,
			[][]int{brands, years},
		)
	case "2.2":
		years := arange(1992, 1999, 1) // all years
		brands := arange(260, 268, 1)  // brands between MFGR#2221 and MFGR#2228 - 7th manufacturer, brands 20-27, 40*(7-1) + [20..27]
		// regionID := 2  // Asia
		qs = NewQuerySet(
			"2.2",
			`Sum(
	Intersect(
		Bitmap(frame="p_brand1", rowID=%d),
		Bitmap(frame="lo_year", rowID=%d),
		Bitmap(frame="s_region", rowID=2),
	),
	frame="lo_revenue", field="lo_revenue")`,
			[][]int{brands, years},
		)

	case "2.3":
		years := arange(1992, 1999, 1) // all years
		// brands := 260               // MFGR#2221
		// regionID := 3               // Europe
		qs = NewQuerySet(
			"2.3",
			`Sum(
	Intersect(
		Bitmap(frame="lo_year", rowID=%d),
		Bitmap(frame="p_brand1", rowID=260),
		Bitmap(frame="s_region", rowID=3),
	),
	frame="lo_revenue", field="lo_revenue")`,
			[][]int{years},
		)

	case "3.1":
		years := arange(1992, 1998, 1)
		nations := arange(10, 15, 1) // asia nations
		qs = NewQuerySet(
			"3.1",
			`Sum(
	Intersect(
		Bitmap(frame="c_nation", rowID=%d),
		Bitmap(frame="s_nation", rowID=%d),
		Bitmap(frame="lo_year", rowID=%d),
	),
	frame="lo_revenue", field="lo_revenue")`,
			[][]int{nations, nations, years},
		)

	case "3.1r":
		years := arange(1992, 1998, 1)
		nations := arange(10, 15, 1) // asia nations
		qs = NewQuerySet(
			"3.1r",
			`Sum(
	Intersect(
		Bitmap(frame="lo_year", rowID=%d),
		IntersectReg(
			Bitmap(frame="c_nation", rowID=%d),
			Bitmap(frame="s_nation", rowID=%d),
		),
	),
	frame="lo_revenue", field="lo_revenue")`,
			[][]int{years, nations, nations},
		)

	case "3.2":
		years := arange(1992, 1998, 1)
		nationID := nations["UNITED STATES"]
		cities := arange(nationID*10, nationID*10+10, 1)
		qs = NewQuerySet(
			"3.2",
			`Sum(
	Intersect(
		Bitmap(frame="c_city", rowID=%d),
		Bitmap(frame="s_city", rowID=%d),
		Bitmap(frame="lo_year", rowID=%d),
	),
	frame="lo_revenue", field="lo_revenue")`,
			[][]int{cities, cities, years},
		)

	case "3.2r":
		years := arange(1992, 1998, 1)
		nationID := nations["UNITED STATES"]
		cities := arange(nationID*10, nationID*10+10, 1)
		qs = NewQuerySet(
			"3.2r",
			`Sum(
	Intersect(
		Bitmap(frame="c_city", rowID=%d),
		IntersectReg(
			Bitmap(frame="s_city", rowID=%d),
			Bitmap(frame="lo_year", rowID=%d),
		),
	),
	frame="lo_revenue", field="lo_revenue")`,
			[][]int{cities, cities, years},
		)

	case "3.3":
		years := arange(1992, 1998, 1)
		cities := []int{181, 185}
		qs = NewQuerySet(
			"3.3",
			`Sum(
	Intersect(
		Bitmap(frame="c_city", rowID=%d),
		Bitmap(frame="s_city", rowID=%d),
		Bitmap(frame="lo_year", rowID=%d),
	),
	frame="lo_revenue", field="lo_revenue")`,
			[][]int{cities, cities, years},
		)

	case "3.4":
		cities := []int{181, 185}
		qs = NewQuerySet(
			"3.4",
			`Sum(
	Intersect(
		Bitmap(frame="c_city", rowID=%d),
		Bitmap(frame="s_city", rowID=%d),
		Bitmap(frame="lo_month", rowID=11),
		Bitmap(frame="lo_year", rowID=1997),
	),
	frame="lo_revenue", field="lo_revenue")`,
			[][]int{cities, cities},
		)

	case "4.1":
		years := arange(1992, 1999, 1)
		nations := arange(0, 5, 1)
		qs = NewQuerySet(
			"4.1",
			`Sum(
	Intersect(
		Bitmap(frame="c_nation", rowID=%d),
		Bitmap(frame="lo_year", rowID=%d),
		Bitmap(frame="s_region", rowID=0),
		Union(
			Bitmap(frame="p_mfgr", rowID=1),
			Bitmap(frame="p_mfgr", rowID=2),
		)
	),
frame="lo_profit", field="lo_profit")`,
			[][]int{nations, years},
		)

	case "4.2":
		years := []int{1997, 1998}
		nations := arange(0, 5, 1)
		categories := arange(0, 10, 1)
		qs = NewQuerySet(
			"4.2",
			`Sum(
	Intersect(
		Bitmap(frame="p_category", rowID=%d),
		Bitmap(frame="s_nation", rowID=%d),
		Bitmap(frame="lo_year", rowID=%d),
		Bitmap(frame="c_region", rowID=0),
	),
frame="lo_profit", field="lo_profit")`,
			[][]int{categories, nations, years},
		)

	case "4.3":
		years := []int{1997, 1998}
		cities := arange(30, 40, 1)
		brands := arange(120, 160, 1)
		qs = NewQuerySet(
			"4.3",
			`Sum(
	Intersect(
		Bitmap(frame="p_brand1", rowID=%d),
		Bitmap(frame="s_city", rowID=%d),
		Bitmap(frame="lo_year", rowID=%d),
		Bitmap(frame="c_region", rowID=0),
	),
frame="lo_profit", field="lo_profit")`,
			[][]int{brands, cities, years},
		)

	case "4.3r":
		years := []int{1997, 1998}
		cities := arange(30, 40, 1)
		brands := arange(120, 160, 1)
		qs = NewQuerySet(
			"4.3r",
			`Sum(
	Intersect(
		Bitmap(frame="p_brand1", rowID=%d),
		IntersectReg(
			Bitmap(frame="lo_year", rowID=%d),
			Bitmap(frame="s_city", rowID=%d),
			Bitmap(frame="c_region", rowID=0),
		),
	),
frame="lo_profit", field="lo_profit")`,
			[][]int{brands, years, cities},
		)

	}

	return qs
}

func (s *Server) HandleTestQuery(w http.ResponseWriter, r *http.Request) {
	qs := NewQuerySet(
		"test",
		`Sum(
	Intersect(
		Bitmap(frame="lo_year", rowID=%d),
		Bitmap(frame="p_brand1", rowID=%d),
		Bitmap(frame="s_region", rowID=%d),
	),
	frame="lo_revenue", field="lo_revenue")`,
		[][]int{
			[]int{1992, 1993, 1994, 1995, 1996, 1997},
			[]int{0, 1, 2, 3},
			[]int{0, 1, 2},
		},
	)

	result := s.RunSumMultiBatch(qs, s.concurrency, s.batchSize)
	enc := json.NewEncoder(w)
	err := enc.Encode(result)
	if err != nil {
		fmt.Printf("writing results: %v to responsewriter: %v", result, err)
	}

}
