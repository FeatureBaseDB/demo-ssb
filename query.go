package main

import (
	"fmt"
	"net/http"
	"sync"
	"time"
)

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

// RunSumBatch collects all queries in a QuerySet into a single batch call, and sends to the cluster.
func (s *Server) RunSumBatch(qs QuerySet) {
	qBatch := ""
	for n := 0; n < qs.iterations; n++ {
		q := qs.QueryN(n)
		qBatch += q
	}

	start := time.Now()
	response, err := s.Client.Query(s.Index.RawQuery(qBatch), nil)
	if err != nil {
		fmt.Printf("batch query failed with: %v", err)
	}
	// TODO sort

	fmt.Printf("response: %T\n", response) // gotta do somethin'
	//for n, res := range response.Results() {
	//	fmt.Println(res.Sum)
	//}
	seconds := time.Now().Sub(start).Seconds()
	fmt.Printf("query %s batch: %d iterations, %f sec\n", qs.Name, qs.iterations, seconds)
}

func (s *Server) RunSumBatchSortable(qs QuerySet) {
	qBatch := ""
	qrs := make([]QueryResult, qs.iterations)
	for n := 0; n < qs.iterations; n++ {
		qrs[n] = qs.QueryResultN(n)
		qBatch += qrs[n].raw
	}

	start := time.Now()
	response, err := s.Client.Query(s.Index.RawQuery(qBatch), nil)
	if err != nil {
		fmt.Printf("batch query failed with: %v", err)
	}

	fmt.Printf("response: %T\n", response) // gotta do somethin'
	for n, res := range response.Results() {
		qrs[n].outputs[0] = res.Sum
		// fmt.Println(res.Sum)
	}
	// TODO sort

	seconds := time.Now().Sub(start).Seconds()
	fmt.Printf("query %s batch: %d iterations, %f sec\n", qs.Name, qs.iterations, seconds)
}

// RunSumConcurrent sends each query in a QuerySet to the cluster concurrently, using runRawSumQuery().
func (s *Server) RunSumConcurrent(qs QuerySet, concurrency int) {
	queries := make(chan string)
	results := make(chan int)
	go func() {
		for n := 0; n < qs.iterations; n++ {
			queries <- qs.QueryN(n)
		}
		close(queries)
	}()

	var wg = &sync.WaitGroup{}
	start := time.Now()
	for n := 0; n < concurrency; n++ {
		wg.Add(1)
		go func() {
			s.runRawSumQuery(queries, results, wg)
		}()
	}
	go func() {
		wg.Wait()
		close(results)
	}()
	// TODO sort

	for res := range results {
		fmt.Println(res)
	}
	seconds := time.Now().Sub(start).Seconds()

	fmt.Printf("query %s concurrent(%d): %d iterations, %f sec\n", qs.Name, concurrency, qs.iterations, seconds)

}

// runRawSumQuery sends one RawQuery to the cluster, then sends the Sum from the result to a result channel
func (s *Server) runRawSumQuery(queries <-chan string, results chan<- int, wg *sync.WaitGroup) {
	defer wg.Done()
	for q := range queries {
		response, err := s.Client.Query(s.Index.RawQuery(q), nil)
		if err != nil {
			fmt.Printf("%v failed with: %v", q, err)
		}
		results <- int(response.Results()[0].Sum)
	}
}

// RunSumMultiBatch sends queries in a QuerySet to the cluster in a configurable combination of
// batchSize and concurrency. Examples:
// concurrency=1, batchSize=(iteration count) -> equivalent to RunSumBatch
// concurrency=N, batchSize=1                 -> equivalent to RunSumConcurrent(N)
// concurrency=N, batchSize=10                -> sends concurrent batches of 10 queries
func (s *Server) RunSumMultiBatch(qs QuerySet, concurrency, batchSize int) {
	queries := make(chan string)
	results := make(chan int)
	go func() {
		qBatch := ""
		batchCount := 0
		for n := 0; n < qs.iterations; n++ {
			if batchCount < batchSize {
				qBatch += qs.QueryN(n)
				batchCount++
			} else {
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

	for res := range results {
		fmt.Println(res)
	}
	seconds := time.Now().Sub(start).Seconds()
	fmt.Printf("query %s concurrent(%d)+batch(%d): %d iterations, %f sec\n", qs.Name, concurrency, batchSize, qs.iterations, seconds)

}

// runRawSumBatchQuery sends RawQueries to the cluster, then sends the Sum from each results to a result channel.
func (s *Server) runRawSumBatchQuery(queries <-chan string, results chan<- int, wg *sync.WaitGroup) {
	defer wg.Done()
	for q := range queries {
		response, err := s.Client.Query(s.Index.RawQuery(q), nil)
		if err != nil {
			fmt.Printf("%v failed with: %v", q, err)
		}
		for _, res := range response.Results() {
			results <- int(res.Sum)
		}
	}
}

func (s *Server) HandleTestQuery(w http.ResponseWriter, r *http.Request) {
	qs := NewQuerySet(
		"test",
		`Sum(
	Intersect(
		Bitmap(frame="lo_year", rowID=%d),
		Bitmap(frame="p_brand1", rowID=%d),
		Bitmap(frame="s_region", rowID=%d)
	),
	frame="lo_revenue", field="lo_revenue")`,
		[][]int{
			[]int{1992, 1993, 1994, 1995, 1996, 1997},
			[]int{0, 1, 2, 3},
			[]int{0, 1, 2},
		},
	)

	s.RunSumBatch(qs)
	s.RunSumBatchSortable(qs)
	s.RunSumConcurrent(qs, 32)
	s.RunSumMultiBatch(qs, 32, 10)
}

func (s *Server) HandleQuery11New(w http.ResponseWriter, r *http.Request) {

}

func (s *Server) HandleQuery12New(w http.ResponseWriter, r *http.Request) {

}

func (s *Server) HandleQuery13New(w http.ResponseWriter, r *http.Request) {

}

func (s *Server) HandleQuery21New(w http.ResponseWriter, r *http.Request) {
	years := arange(1992, 1999, 1) // all years
	brands := arange(40, 80, 1)    // brands for the second manufacturer, "MFGR#12"
	// regionID := 0  // America
	qs := NewQuerySet(
		"2.1",
		`Sum(
	Intersect(
		Bitmap(frame="lo_year", rowID=%d),
		Bitmap(frame="p_brand1", rowID=%d),
		Bitmap(frame="s_region", rowID=0)
	),
	frame="lo_revenue", field="lo_revenue")`,
		[][]int{years, brands},
	)
	s.RunSumBatch(qs)
	s.RunSumConcurrent(qs, 32)
	s.RunSumMultiBatch(qs, 32, 10)
}

func (s *Server) HandleQuery22New(w http.ResponseWriter, r *http.Request) {
	years := arange(1992, 1999, 1) // all years
	brands := arange(260, 268, 1)  // brands between MFGR#2221 and MFGR#2228 - 7th manufacturer, brands 20-27, 40*(7-1) + [20..27]
	// regionID := 2  // Asia
	qs := NewQuerySet(
		"2.2",
		`Sum(
	Intersect(
		Bitmap(frame="lo_year", rowID=%d),
		Bitmap(frame="p_brand1", rowID=%d),
		Bitmap(frame="s_region", rowID=2)
	),
	frame="lo_revenue", field="lo_revenue")`,
		[][]int{years, brands},
	)
	s.RunSumBatch(qs)
	s.RunSumConcurrent(qs, 32)
	s.RunSumMultiBatch(qs, 32, 10)
}

func (s *Server) HandleQuery23New(w http.ResponseWriter, r *http.Request) {
	years := arange(1992, 1999, 1) // all years
	// brands := 260               // MFGR#2221
	// regionID := 3               // Europe
	qs := NewQuerySet(
		"2.3",
		`Sum(
	Intersect(
		Bitmap(frame="lo_year", rowID=%d),
		Bitmap(frame="p_brand1", rowID=260),
		Bitmap(frame="s_region", rowID=3)
	),
	frame="lo_revenue", field="lo_revenue")`,
		[][]int{years},
	)
	s.RunSumBatch(qs)
	s.RunSumConcurrent(qs, 32)
	s.RunSumMultiBatch(qs, 32, 10)

}

func (s *Server) HandleQuery31New(w http.ResponseWriter, r *http.Request) {
	years := arange(1992, 1998, 1)
	nations := arange(10, 15, 1) // asia nations
	qs := NewQuerySet(
		"3.1",
		`Sum(
	Intersect(
		Bitmap(frame="lo_year", rowID=%d),
		Bitmap(frame="c_nation", rowID=%d),
		Bitmap(frame="s_nation", rowID=%d)
	),
	frame="lo_revenue", field="lo_revenue")`,
		[][]int{years, nations, nations},
	)
	s.RunSumBatch(qs)
	s.RunSumConcurrent(qs, 32)
	s.RunSumMultiBatch(qs, 32, 10)
}

func (s *Server) HandleQuery32New(w http.ResponseWriter, r *http.Request) {
	years := arange(1992, 1998, 1)
	nationID := nations["UNITED STATES"]
	cities := arange(nationID*10, nationID*10+10, 1)
	qs := NewQuerySet(
		"3.2",
		`Sum(
	Intersect(
		Bitmap(frame="lo_year", rowID=%d),
		Bitmap(frame="c_city", rowID=%d),
		Bitmap(frame="s_city", rowID=%d)
	),
	frame="lo_revenue", field="lo_revenue")`,
		[][]int{years, cities, cities},
	)
	s.RunSumBatch(qs)
	s.RunSumConcurrent(qs, 32)
	s.RunSumMultiBatch(qs, 32, 10)
}

func (s *Server) HandleQuery33New(w http.ResponseWriter, r *http.Request) {
	years := arange(1992, 1998, 1)
	cities := []int{181, 185}
	qs := NewQuerySet(
		"3.3",
		`Sum(
	Intersect(
		Bitmap(frame="lo_year", rowID=%d),
		Bitmap(frame="c_city", rowID=%d),
		Bitmap(frame="s_city", rowID=%d)
	),
	frame="lo_revenue", field="lo_revenue")`,
		[][]int{years, cities, cities},
	)
	s.RunSumBatch(qs)
	s.RunSumConcurrent(qs, 32)
	s.RunSumMultiBatch(qs, 32, 10)
}

func (s *Server) HandleQuery34New(w http.ResponseWriter, r *http.Request) {
	cities := []int{181, 185}
	qs := NewQuerySet(
		"3.4",
		`Sum(
	Intersect(
		Bitmap(frame="lo_year", rowID=5),
		Bitmap(frame="lo_month", rowID=11),
		Bitmap(frame="c_city", rowID=%d),
		Bitmap(frame="s_city", rowID=%d)
	),
	frame="lo_revenue", field="lo_revenue")`,
		[][]int{cities, cities},
	)
	s.RunSumBatch(qs)
	s.RunSumConcurrent(qs, 32)
	s.RunSumMultiBatch(qs, 32, 10)
}

func (s *Server) HandleQuery41New(w http.ResponseWriter, r *http.Request) {
	years := arange(1991, 1999, 1)
	nations := arange(0, 5, 1)
	qs := NewQuerySet(
		"4.1",
		`Sum(
	Intersect(
		Bitmap(frame="lo_year", rowID=%d),
		Bitmap(frame="c_nation", rowID=%d),
		Bitmap(frame="s_region", rowID=0),
		Union(
			Bitmap(frame="p_mfgr", rowID=1),
			Bitmap(frame="p_mfgr", rowID=2)
		)
	),
frame="lo_profit", field="lo_profit")`,
		[][]int{years, nations},
	)
	s.RunSumBatch(qs)
	s.RunSumConcurrent(qs, 32)
	s.RunSumMultiBatch(qs, 32, 10)
}

func (s *Server) HandleQuery42New(w http.ResponseWriter, r *http.Request) {
	years := []int{1997, 1998}
	nations := arange(0, 5, 1)
	categories := arange(0, 10, 1)
	qs := NewQuerySet(
		"4.2",
		`Sum(
	Intersect(
		Bitmap(frame="lo_year", rowID=%d),
		Bitmap(frame="s_nation", rowID=%d),
		Bitmap(frame="c_region", rowID=0),
		Bitmap(frame="p_category", rowID=%d),
	),
frame="lo_profit", field="lo_profit")`,
		[][]int{years, nations, categories},
	)
	s.RunSumBatch(qs)
	s.RunSumConcurrent(qs, 32)
	s.RunSumMultiBatch(qs, 32, 10)
}

func (s *Server) HandleQuery43New(w http.ResponseWriter, r *http.Request) {
	years := []int{1997, 1998}
	cities := arange(30, 40, 1)
	brands := arange(120, 160, 1)
	qs := NewQuerySet(
		"4.3",
		`Sum(
	Intersect(
		Bitmap(frame="lo_year", rowID=%d),
		Bitmap(frame="s_city", rowID=%d),
		Bitmap(frame="c_region", rowID=0),
		Bitmap(frame="p_brand1", rowID=%d),
	),
frame="lo_profit", field="lo_profit")`,
		[][]int{years, cities, brands},
	)
	s.RunSumBatch(qs)
	s.RunSumConcurrent(qs, 32)
	s.RunSumMultiBatch(qs, 32, 10)
}
