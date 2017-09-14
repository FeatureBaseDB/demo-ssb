package main

import (
	"fmt"
	"net/http"
	"time"
)

/*
3.1  group by (c_nation, s_nation, year)  order by (year asc, revenue desc)
3.2+ group by (c_city, s_city, year)      order by (year asc, revenue desc)
*/
func (s *Server) HandleQuery31(w http.ResponseWriter, r *http.Request) {
	start := time.Now()

	seconds := time.Now().Sub(start).Seconds()
	fmt.Printf("query 3.1: %f sec\n", seconds)
}

func (s *Server) HandleQuery32(w http.ResponseWriter, r *http.Request) {
	start := time.Now()

	seconds := time.Now().Sub(start).Seconds()
	fmt.Printf("query 3.2: %f sec\n", seconds)
}

func (s *Server) HandleQuery33(w http.ResponseWriter, r *http.Request) {
	start := time.Now()

	seconds := time.Now().Sub(start).Seconds()
	fmt.Printf("query 3.3: %f sec\n", seconds)
}
func (s *Server) HandleQuery34(w http.ResponseWriter, r *http.Request) {
	start := time.Now()

	seconds := time.Now().Sub(start).Seconds()
	fmt.Printf("query 3.4: %f sec\n", seconds)
}

type query31Row struct {
	RevenueSum int
	Year       int
	YearID     int
	CNationID  int
	CNation    string
	SNationID  int
	SNation    string
}

func (q query31Row) String() string {
	return fmt.Sprintf("<query3.1 year=%d cnation=%s snation=%s, sum=%d>", q.Year, q.CNation, q.SNation, q.RevenueSum)
}

type query32Row struct {
	RevenueSum int
	Year       int
	YearID     int
	CCityID    int
	CCity      string
	SCityID    int
	SCity      string
}

func (q query32Row) String() string {
	return fmt.Sprintf("<query3.2 year=%d ccity=%s scity=%s, sum=%d>", q.Year, q.CCity, q.SCity, q.RevenueSum)
}

type byYearRevenue []query31Row

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

// TODO use byYearRevenue for both query31Row and query32Row?
