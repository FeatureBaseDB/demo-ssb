package main

import (
	"fmt"
	"log"
	"net/http"
	"sort"
	"sync"
	"time"
)

// TODO consolidate HandleQuery2* to use a single function that accepts
// a slice of brandIDs. logic for handling brandnum, brandID, brand correctly
// is not necessary.

func (s *Server) HandleQuery21(w http.ResponseWriter, r *http.Request) {
	iterations := 280
	concurrency := 32

	start := time.Now()
	keys := make(chan query2Row)
	rows := make(chan query2Row)
	go func() {
		// group by (year, brand)
		for year, yearID := range yearMap {
			for brandnum := 0; brandnum < 40; brandnum++ {
				brand := fmt.Sprintf("MFGR#12%d", brandnum+1)
				// category #1 is 11, has brands 1-40, ids 0-39
				// category #2 is 12, has brands 41-80, ids 40-79
				brandID := brandnum + 40*(2-1)
				keys <- query2Row{0, year, yearID, brandnum, brand, brandID}
			}
		}
		close(keys)
	}()

	var wg = &sync.WaitGroup{}
	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func() {
			s.RunQuery2(keys, rows, wg, regions["AMERICA"])
		}()
	}
	go func() {
		wg.Wait()
		close(rows)
	}()

	resp := make([]query2Row, 0, iterations)
	for row := range rows {
		resp = append(resp, row)
	}

	// order by (year, brand)
	sort.Sort(byYearBrandnum(resp))
	seconds := time.Now().Sub(start).Seconds()

	for _, row := range resp {
		fmt.Println(row)
	}

	fmt.Printf("query 2.1: %f sec\n", seconds)
}

func (s *Server) HandleQuery22(w http.ResponseWriter, r *http.Request) {
	iterations := 56
	concurrency := 32

	start := time.Now()
	keys := make(chan query2Row)
	rows := make(chan query2Row)
	go func() {
		// group by (year, brand)
		for year, yearID := range yearMap {
			// brands 2221-2228 -> category 22, brandnums 20-27
			for brandnum := 20; brandnum <= 27; brandnum++ {
				brand := fmt.Sprintf("MFGR#22%d", brandnum+1)
				// category #1 is 11, has brands 1-40, ids 0-39
				// category #2 is 12, has brands 41-80, ids 40-79
				// category #6 is 21
				// category #7 is 22
				brandID := brandnum + 40*(7-1)
				keys <- query2Row{0, year, yearID, brandnum, brand, brandID}
			}
		}
		close(keys)
	}()

	var wg = &sync.WaitGroup{}
	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func() {
			s.RunQuery2(keys, rows, wg, regions["ASIA"])
		}()
	}
	go func() {
		wg.Wait()
		close(rows)
	}()

	resp := make([]query2Row, 0, iterations)
	for row := range rows {
		resp = append(resp, row)
	}

	// order by (year, brand)
	sort.Sort(byYearBrandnum(resp))
	seconds := time.Now().Sub(start).Seconds()

	for _, row := range resp {
		fmt.Println(row)
	}

	fmt.Printf("query 2.2: %f sec\n", seconds)
}
func (s *Server) HandleQuery23(w http.ResponseWriter, r *http.Request) {
	iterations := 7
	concurrency := 7

	start := time.Now()
	keys := make(chan query2Row)
	rows := make(chan query2Row)
	go func() {
		// group by (year, brand)
		for year, yearID := range yearMap {
			brandnum := 20
			brand := fmt.Sprintf("MFGR#22%d", brandnum+1)
			brandID := brandnum + 40*(7-1)
			keys <- query2Row{0, year, yearID, brandnum, brand, brandID}
		}
		close(keys)
	}()

	var wg = &sync.WaitGroup{}
	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func() {
			s.RunQuery2(keys, rows, wg, regions["EUROPE"])
		}()
	}
	go func() {
		wg.Wait()
		close(rows)
	}()

	resp := make([]query2Row, 0, iterations)
	for row := range rows {
		resp = append(resp, row)
	}

	// order by (year, brand)
	sort.Sort(byYearBrandnum(resp))
	seconds := time.Now().Sub(start).Seconds()

	for _, row := range resp {
		fmt.Println(row)
	}

	fmt.Printf("query 2.3: %f sec\n", seconds)
}

const q2Fmt = `
Sum(
	Intersect(
		Bitmap(frame="lo_year", rowID=%d),
		Bitmap(frame="p_brand1", rowID=%d),
		Bitmap(frame="s_region", rowID=%d)
	),
	frame="lo_revenue", field="lo_revenue")`

func (s *Server) RunQuery2(keys <-chan query2Row, rows chan<- query2Row, wg *sync.WaitGroup, region int) {
	defer wg.Done()
	for key := range keys {
		q := fmt.Sprintf(q2Fmt, key.YearID, key.BrandID, region)
		// fmt.Println(q)
		response, err := s.Client.Query(s.Index.RawQuery(q), nil)
		if err != nil {
			log.Printf("%v failed with: %v", key, err)
			// return
		}
		row := key
		row.RevenueSum = int(response.Results()[0].Sum)
		// fmt.Println(response.Results()[0].Sum)
		rows <- row
	}
}

/*
2.*  group by (year, brand),              order by (year, brand)
*/

type query2Row struct {
	RevenueSum int
	Year       int
	YearID     int
	Brandnum   int
	Brand      string
	BrandID    int
}

func (q query2Row) String() string {
	return fmt.Sprintf("<query2.X year=%d brandnum=%2d sum=%d>", q.Year, q.Brandnum, q.RevenueSum)
}

type byYearBrandnum []query2Row

func (a byYearBrandnum) Len() int      { return len(a) }
func (a byYearBrandnum) Swap(i, j int) { a[i], a[j] = a[j], a[i] }
func (a byYearBrandnum) Less(i, j int) bool {
	if a[i].Year < a[j].Year {
		return true
	}
	if a[i].Year == a[j].Year && a[i].Brandnum < a[j].Brandnum {
		return true
	}
	return false
}
