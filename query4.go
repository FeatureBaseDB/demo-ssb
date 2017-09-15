package main

import (
	"fmt"
	"log"
	"net/http"
	"sort"
	"sync"
	"time"
)

var q4years = []int{1997, 1998}

func (s *Server) HandleQuery41(w http.ResponseWriter, r *http.Request) {
	iterations := 35
	concurrency := 32
	start := time.Now()

	keys := make(chan query4Row)
	rows := make(chan query4Row)
	go func() {
		// group
		for _, year := range yearMap {
			for _, cNation := range americaNations {
				cNationID := nations[cNation]
				keys <- NewQuery41Row(year, cNationID)
			}
		}
		close(keys)
	}()

	var wg = &sync.WaitGroup{}
	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func() {
			s.RunQuery41(keys, rows, wg)
		}()
	}
	go func() {
		wg.Wait()
		close(rows)
	}()

	resp := make([]query4Row, 0, iterations)
	for row := range rows {
		resp = append(resp, row)
	}

	// order
	sort.Sort(byYearCNation(resp))
	seconds := time.Now().Sub(start).Seconds()

	//for _, row := range resp {
	//	fmt.Println(row)
	//}

	fmt.Printf("query 4.1: %f sec\n", seconds)
}
func (s *Server) HandleQuery42(w http.ResponseWriter, r *http.Request) {
	iterations := 100
	concurrency := 32
	start := time.Now()
	keys := make(chan query4Row)
	rows := make(chan query4Row)
	go func() {
		// group
		for _, year := range q4years {
			for _, sNation := range americaNations {
				sNationID := nations[sNation]
				for categoryID := 0; categoryID < 10; categoryID++ {
					keys <- NewQuery42Row(year, sNationID, categoryID)
				}
			}
		}
		close(keys)
	}()

	var wg = &sync.WaitGroup{}
	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func() {
			s.RunQuery42(keys, rows, wg)
		}()
	}
	go func() {
		wg.Wait()
		close(rows)
	}()

	resp := make([]query4Row, 0, iterations)
	for row := range rows {
		resp = append(resp, row)
	}

	// order
	sort.Sort(byYearSnationCategory(resp))
	seconds := time.Now().Sub(start).Seconds()

	//for _, row := range resp {
	//	fmt.Println(row)
	//}

	fmt.Printf("query 4.2: %f sec\n", seconds)
}
func (s *Server) HandleQuery43(w http.ResponseWriter, r *http.Request) {
	iterations := 800
	concurrency := 32
	start := time.Now()

	keys := make(chan query4Row)
	rows := make(chan query4Row)
	go func() {
		// group
		for _, year := range q4years {
			for cityID := 30; cityID < 40; cityID++ {
				for brandID := 120; brandID < 160; brandID++ {
					keys <- NewQuery43Row(year, cityID, brandID)
				}
			}
		}
		close(keys)
	}()

	var wg = &sync.WaitGroup{}
	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func() {
			s.RunQuery42(keys, rows, wg)
		}()
	}
	go func() {
		wg.Wait()
		close(rows)
	}()

	resp := make([]query4Row, 0, iterations)
	for row := range rows {
		resp = append(resp, row)
	}

	// order
	sort.Sort(byYearScityBrand(resp))
	seconds := time.Now().Sub(start).Seconds()
	//for _, row := range resp {
	//	fmt.Println(row)
	//}

	fmt.Printf("query 4.3: %f sec\n", seconds)
}

func (s *Server) RunQuery41(keys <-chan query4Row, rows chan<- query4Row, wg *sync.WaitGroup) {
	defer wg.Done()
	for key := range keys {
		q := fmt.Sprintf(q41Fmt, key.Year, key.CNation)
		response, err := s.Client.Query(s.Index.RawQuery(q), nil)
		if err != nil {
			log.Printf("%v failed with: %v", key, err)
			// return
		}
		row := key
		//fmt.Println(q)
		//fmt.Printf("%T %+v\n", response.Results()[0], response.Results()[0])
		row.ProfitSum = int(response.Results()[0].Sum)
		rows <- row
	}
}
func (s *Server) RunQuery42(keys <-chan query4Row, rows chan<- query4Row, wg *sync.WaitGroup) {
	defer wg.Done()
	for key := range keys {
		q := fmt.Sprintf(q42Fmt, key.Year, key.SNation, key.Category)
		response, err := s.Client.Query(s.Index.RawQuery(q), nil)
		if err != nil {
			log.Printf("%v failed with: %v", key, err)
			// return
		}
		row := key
		row.ProfitSum = int(response.Results()[0].Sum)
		rows <- row
	}
}

func (s *Server) RunQuery43(keys <-chan query4Row, rows chan<- query4Row, wg *sync.WaitGroup) {
	defer wg.Done()
	for key := range keys {
		q := fmt.Sprintf(q43Fmt, key.Year, key.SCity, key.Brand)
		response, err := s.Client.Query(s.Index.RawQuery(q), nil)
		if err != nil {
			log.Printf("%v failed with: %v", key, err)
			// return
		}
		row := key
		row.ProfitSum = int(response.Results()[0].Sum)
		rows <- row
	}
}

func NewQuery41Row(year, nation int) query4Row {
	row := query4Row{}
	row.Year, row.CNation = year, nation
	return row
}

func NewQuery42Row(year, snation, category int) query4Row {
	row := query4Row{}
	row.Year, row.SNation, row.Category = year, snation, category
	return row
}

func NewQuery43Row(year, scity, brand int) query4Row {
	row := query4Row{}
	row.Year, row.SCity, row.Brand = year, scity, brand
	return row
}

/*
4.1  group by (year, c_nation)            order by (year, c_nation)
4.2  group by (year, s_nation, category), order by (year, s_nation, category)
4.3  group by (year, s_city, brand),      order by (year, s_city, brand)
*/

type query4Row struct {
	ProfitSum int
	Year      int
	CNation   int
	SNation   int
	SCity     int
	Mfgr1     int
	Mfgr2     int
	Category  int
	Brand     int
}

const q41Fmt = `
Sum(
	Intersect(
		Bitmap(frame="lo_year", rowID=%d),
		Bitmap(frame="c_nation", rowID=%d),
		Bitmap(frame="s_region", rowID=0),
		Union(
			Bitmap(frame="p_mfgr", rowID=1),
			Bitmap(frame="p_mfgr", rowID=2)
		)
	),
frame="lo_profit", field="lo_profit")`

const q42Fmt = `
Sum(
	Intersect(
		Bitmap(frame="lo_year", rowID=%d),
		Bitmap(frame="s_nation", rowID=%d),
		Bitmap(frame="c_region", rowID=0),
		Bitmap(frame="p_category", rowID=%d),
	),
frame="lo_profit", field="lo_profit")`

const q43Fmt = `
Sum(
	Intersect(
		Bitmap(frame="lo_year", rowID=%d),
		Bitmap(frame="s_city", rowID=%d),
		Bitmap(frame="c_region", rowID=0),
		Bitmap(frame="p_brand1", rowID=%d),
	),
frame="lo_profit", field="lo_profit")`

func (q query4Row) String() string {
	return fmt.Sprintf("<query4.X sum=%d>", q.ProfitSum)
}

type byYearCNation []query4Row

func (a byYearCNation) Len() int      { return len(a) }
func (a byYearCNation) Swap(i, j int) { a[i], a[j] = a[j], a[i] }
func (a byYearCNation) Less(i, j int) bool {
	if a[i].Year < a[j].Year {
		return true
	}
	if a[i].Year == a[j].Year && a[i].CNation < a[j].CNation {
		return true
	}
	return false
}

type byYearSnationCategory []query4Row

func (a byYearSnationCategory) Len() int      { return len(a) }
func (a byYearSnationCategory) Swap(i, j int) { a[i], a[j] = a[j], a[i] }
func (a byYearSnationCategory) Less(i, j int) bool {
	if a[i].Year < a[j].Year {
		return true
	}
	if a[i].Year == a[j].Year && a[i].SNation < a[j].SNation {
		return true
	}
	if a[i].Year == a[j].Year && a[i].SNation == a[j].SNation && a[i].Category < a[j].Category {
		return true
	}
	return false
}

type byYearScityBrand []query4Row

func (a byYearScityBrand) Len() int      { return len(a) }
func (a byYearScityBrand) Swap(i, j int) { a[i], a[j] = a[j], a[i] }
func (a byYearScityBrand) Less(i, j int) bool {
	if a[i].Year < a[j].Year {
		return true
	}
	if a[i].Year == a[j].Year && a[i].SCity < a[j].SCity {
		return true
	}
	if a[i].Year == a[j].Year && a[i].SCity == a[j].SCity && a[i].Brand < a[j].Brand {
		return true
	}
	return false
}
