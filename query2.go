package main

import (
	"fmt"
	"log"
	"net/http"
	"sort"
	"sync"
	"time"
)

func (s *Server) HandleQuery21(w http.ResponseWriter, r *http.Request) {
	iterations := 280
	concurrency := 32

	start := time.Now()
	keys := make(chan query21Row)
	rows := make(chan query21Row)
	go func() {
		// group by (year, brand)
		for year, yearID := range years {
			for brandnum := 1; brandnum <= 40; brandnum++ {
				brand := fmt.Sprintf("MFGR#12%d", brandnum)
				// category #1 is 11, has brands 1-40, ids 0-39
				// category #1 is 12, has brands 41-80, ids 40-79
				brandID := brandnum + 40*1
				keys <- query21Row{0, year, yearID, brandnum, brand, brandID}
			}
		}
		close(keys)
	}()

	var wg = &sync.WaitGroup{}
	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func() {
			s.RunQuery21(keys, rows, wg)
		}()
	}
	go func() {
		wg.Wait()
		close(rows)
	}()

	resp := make([]query21Row, 0, iterations)
	for row := range rows {
		resp = append(resp, row)
	}

	// order by (year, brand)
	sort.Sort(byYearBrandnum(resp))

	for _, row := range resp {
		fmt.Println(row)
	}

	seconds := time.Now().Sub(start).Seconds()
	fmt.Printf("query 2.1: %f sec\n", seconds)
}

func (s *Server) HandleQuery22(w http.ResponseWriter, r *http.Request) {
	start := time.Now()

	seconds := time.Now().Sub(start).Seconds()
	fmt.Printf("query 2.2: %f sec\n", seconds)
}
func (s *Server) HandleQuery23(w http.ResponseWriter, r *http.Request) {
	start := time.Now()

	seconds := time.Now().Sub(start).Seconds()
	fmt.Printf("query 2.3: %f sec\n", seconds)
}

const q21Fmt = `
Sum(
	Intersect(
		Bitmap(frame="d_year", rowID=%d),
		Bitmap(frame="p_brand1", rowID=%d),
		Bitmap(frame="s_region", rowID=%d)
	),
	frame="bsi", field="lo_revenue")`

func (s *Server) RunQuery21(keys <-chan query21Row, rows chan<- query21Row, wg *sync.WaitGroup) {
	defer wg.Done()
	for key := range keys {
		q := fmt.Sprintf(q21Fmt, key.YearID, key.BrandID, regions["AMERICA"])
		response, err := s.Client.Query(s.Index.RawQuery(q), nil)
		if err != nil {
			log.Printf("%v failed with: %v", key, err)
			// return
		}
		fmt.Println(response)
		row := key
		//row.RevenueSum = response.sum
		row.RevenueSum = -1
		rows <- row
	}
}

/*
2.*  group by (year, brand),              order by (year, brand)
*/

type query21Row struct {
	RevenueSum int
	Year       int
	YearID     int
	Brandnum   int
	Brand      string
	BrandID    int
}

func (q query21Row) String() string {
	return fmt.Sprintf("<query2.1 year=%d brandnum=%2d sum=%d>", q.Year, q.Brandnum, q.RevenueSum)
}

type byYearBrandnum []query21Row

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
