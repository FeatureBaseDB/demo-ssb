package main

import (
	"fmt"
	"log"
	"net/http"
	"sort"
	"sync"
	"time"
)

/*
3.1  group by (c_nation, s_nation, year)  order by (year asc, revenue desc)
3.2+ group by (c_city, s_city, year)      order by (year asc, revenue desc)
*/
var q3years = []int{1992, 1993, 1994, 1995, 1996, 1997}

func (s *Server) HandleQuery31(w http.ResponseWriter, r *http.Request) {
	iterations := 150
	start := time.Now()

	keys := make(chan query3Row)
	rows := make(chan query3Row)
	go func() {
		// group by (c_nation, s_nation, d_year)
		for _, year := range q3years {
			yearID := yearMap[year]
			for _, cNation := range asiaNations {
				cNationID := nations[cNation]
				for _, sNation := range asiaNations {
					sNationID := nations[cNation]
					keys <- NewQuery31Row(year, yearID, cNationID, sNationID, cNation, sNation)
				}
			}
		}
		close(keys)
	}()

	var wg = &sync.WaitGroup{}
	for i := 0; i < s.concurrency; i++ {
		wg.Add(1)
		go func() {
			s.RunQuery31(keys, rows, wg)
		}()
	}
	go func() {
		wg.Wait()
		close(rows)
	}()

	resp := make([]query3Row, 0, iterations)
	for row := range rows {
		resp = append(resp, row)
	}

	// order by (year, revenue)
	sort.Sort(byYearRevenue(resp))
	seconds := time.Now().Sub(start).Seconds()

	//for _, row := range resp {
	//	fmt.Println(row)
	//}

	fmt.Printf("query 3.1: %f sec\n", seconds)
}

const q31Fmt = `
Sum(
	Intersect(
		Bitmap(frame="lo_year", rowID=%d),
		Bitmap(frame="c_nation", rowID=%d),
		Bitmap(frame="s_nation", rowID=%d)
	),
	frame="lo_revenue", field="lo_revenue")`

func (s *Server) RunQuery31(keys <-chan query3Row, rows chan<- query3Row, wg *sync.WaitGroup) {
	defer wg.Done()
	for key := range keys {
		q := fmt.Sprintf(q31Fmt, key.YearID, key.CNationID, key.SNationID)
		response, err := s.Client.Query(s.Index.RawQuery(q), nil)
		if err != nil {
			log.Printf("%v failed with: %v", key, err)
			// return
		}
		row := key
		row.RevenueSum = int(response.Results()[0].Sum)
		rows <- row
	}
}

func (s *Server) HandleQuery3City(years, ccities, scities []int, qname string) {
	iterations := len(years) * len(ccities) * len(scities)
	start := time.Now()

	keys := make(chan query3Row)
	rows := make(chan query3Row)
	go func() {
		// group by (c_nation, s_nation, d_year)
		for _, year := range years {
			yearID := yearMap[year]
			for _, cCityID := range ccities {
				cCity := cityIDs[cCityID]
				for _, sCityID := range scities {
					sCity := cityIDs[sCityID]
					keys <- NewQuery32Row(year, yearID, cCityID, sCityID, cCity, sCity)
				}
			}
		}
		close(keys)
	}()

	var wg = &sync.WaitGroup{}
	for i := 0; i < s.concurrency; i++ {
		wg.Add(1)
		go func() {
			s.RunQuery32(keys, rows, wg)
		}()
	}
	go func() {
		wg.Wait()
		close(rows)
	}()

	resp := make([]query3Row, 0, iterations)
	for row := range rows {
		resp = append(resp, row)
	}

	// order by (year, revenue)
	sort.Sort(byYearRevenue(resp))
	seconds := time.Now().Sub(start).Seconds()

	//for _, row := range resp {
	//	fmt.Println(row)
	//}

	fmt.Printf("query %s: %f sec\n", qname, seconds)
}

func (s *Server) HandleQuery3CityMonth(yearMonths, ccities, scities []int, qname string) {
	iterations := len(yearMonths) * len(ccities) * len(scities)
	start := time.Now()

	keys := make(chan query3Row)
	rows := make(chan query3Row)
	go func() {
		// group by (c_nation, s_nation, d_year)
		for _, yearMonth := range yearMonths {
			for _, cCityID := range ccities {
				cCity := cityIDs[cCityID]
				for _, sCityID := range scities {
					sCity := cityIDs[sCityID]
					keys <- NewQuery34Row(yearMonth, cCityID, sCityID, cCity, sCity)
				}
			}
		}
		close(keys)
	}()

	var wg = &sync.WaitGroup{}
	for i := 0; i < s.concurrency; i++ {
		wg.Add(1)
		go func() {
			s.RunQuery34(keys, rows, wg)
		}()
	}
	go func() {
		wg.Wait()
		close(rows)
	}()

	resp := make([]query3Row, 0, iterations)
	for row := range rows {
		resp = append(resp, row)
	}

	// order by (year, revenue)
	sort.Sort(byYearRevenue(resp))
	seconds := time.Now().Sub(start).Seconds()

	//for _, row := range resp {
	//	fmt.Println(row)
	//}

	fmt.Printf("query %s: %f sec\n", qname, seconds)
}

func (s *Server) HandleQuery32(w http.ResponseWriter, r *http.Request) {
	nationID := nations["UNITED STATES"]
	cityMin, cityMax := nationID*10, nationID*10+10
	cities := make([]int, 0, 10)
	for i := cityMin; i < cityMax; i++ {
		cities = append(cities, i)
		// fmt.Println(i, cityIDs[i])
	}
	s.HandleQuery3City(q3years, cities, cities, "3.2")
}

func (s *Server) HandleQuery33(w http.ResponseWriter, r *http.Request) {
	cities := []int{181, 185} // UK 1 and UK 5
	s.HandleQuery3City(q3years, cities, cities, "3.3")
}

func (s *Server) HandleQuery34(w http.ResponseWriter, r *http.Request) {
	yearMonths := []int{71}   // dec 1997
	cities := []int{181, 185} // UK 1 and UK 5
	s.HandleQuery3CityMonth(yearMonths, cities, cities, "3.4")
}

func NewQuery31Row(year, yearID, cNationID, sNationID int, cNation, sNation string) query3Row {
	row := query3Row{}
	row.Year, row.YearID = year, yearID
	row.CNation, row.CNationID = cNation, cNationID
	row.SNation, row.SNationID = sNation, sNationID
	return row
}

func NewQuery32Row(year, yearID, cCityID, sCityID int, cCity, sCity string) query3Row {
	row := query3Row{}
	row.Year, row.YearID = year, yearID
	row.CCity, row.CCityID = cCity, cCityID
	row.SCity, row.SCityID = sCity, sCityID
	return row
}

func NewQuery34Row(yearMonthID, cCityID, sCityID int, cCity, sCity string) query3Row {
	row := query3Row{}
	row.YearMonthID = yearMonthID
	row.CCity, row.CCityID = cCity, cCityID
	row.SCity, row.SCityID = sCity, sCityID
	return row
}

const q32Fmt = `
Sum(
	Intersect(
		Bitmap(frame="lo_year", rowID=%d),
		Bitmap(frame="c_city", rowID=%d),
		Bitmap(frame="s_city", rowID=%d)
	),
	frame="lo_revenue", field="lo_revenue")`

func (s *Server) RunQuery32(keys <-chan query3Row, rows chan<- query3Row, wg *sync.WaitGroup) {
	defer wg.Done()
	for key := range keys {
		q := fmt.Sprintf(q32Fmt, key.YearID, key.CCityID, key.SCityID)
		response, err := s.Client.Query(s.Index.RawQuery(q), nil)
		if err != nil {
			log.Printf("%v failed with: %v", key, err)
			// return
		}
		row := key
		row.RevenueSum = int(response.Results()[0].Sum)
		rows <- row
	}
}

const q34Fmt = `
Sum(
	Intersect(
		Bitmap(frame="lo_year", rowID=5),
		Bitmap(frame="lo_month", rowID=11),
		Bitmap(frame="c_city", rowID=%d),
		Bitmap(frame="s_city", rowID=%d)
	),
	frame="lo_revenue", field="lo_revenue")`

func (s *Server) RunQuery34(keys <-chan query3Row, rows chan<- query3Row, wg *sync.WaitGroup) {
	defer wg.Done()
	for key := range keys {
		q := fmt.Sprintf(q34Fmt, key.CCityID, key.SCityID)
		response, err := s.Client.Query(s.Index.RawQuery(q), nil)
		if err != nil {
			log.Printf("%v failed with: %v", key, err)
			// return
		}
		row := key
		row.RevenueSum = int(response.Results()[0].Sum)
		rows <- row
	}
}

type query3Row struct {
	RevenueSum  int
	Year        int
	YearID      int
	YearMonthID int
	CNationID   int
	CNation     string
	SNationID   int
	SNation     string
	CCityID     int
	CCity       string
	SCityID     int
	SCity       string
}

func (q query3Row) String() string {
	return fmt.Sprintf("<query3.2 year=%d-%d %s-%s %s-%s, sum=%d>", q.Year, q.YearMonthID, q.CNation, q.CCity, q.SNation, q.SCity, q.RevenueSum)
}

type byYearRevenue []query3Row

func (a byYearRevenue) Len() int      { return len(a) }
func (a byYearRevenue) Swap(i, j int) { a[i], a[j] = a[j], a[i] }
func (a byYearRevenue) Less(i, j int) bool {
	if a[i].Year < a[j].Year {
		return true
	}
	if a[i].Year == a[j].Year && a[i].RevenueSum > a[j].RevenueSum {
		return true
	}
	return false
}
