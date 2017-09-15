package main

import (
	"fmt"
	"net/http"
	"time"
)

// rowID=1 -> year=1993
const q11 = `
Sum(
	Intersect(
		Bitmap(frame="lo_year", rowID=1),
		Range(frame="lo_discount", lo_discount >= 1),
		Range(frame="lo_discount", lo_discount <= 3),
		Range(frame="lo_quantity", lo_quantity < 25)
	),
frame="lo_revenue_computed", field="lo_revenue_computed")`

// rowID = 24 -> yearmonth=199801
const q12 = `
Sum(
	Intersect(
		Bitmap(frame="lo_year", rowID=6),
		Bitmap(frame="lo_month", rowID=0),
		Range(frame="lo_discount", lo_discount >= 4),
		Range(frame="lo_discount", lo_discount <= 6),
		Range(frame="lo_quantity", lo_quantity >= 26),
		Range(frame="lo_quantity", lo_quantity <= 35)
	),
frame="lo_revenue_computed", field="lo_revenue_computed")`

// rowID=2 -> year=1994
const q13 = `
Sum(
	Intersect(
		Bitmap(frame="lo_weeknum", rowID=6),
		Bitmap(frame="lo_year", rowID=2),
		Range(frame="lo_discount", lo_discount >= 5),
		Range(frame="lo_discount", lo_discount <= 7),
		Range(frame="lo_quantity", lo_quantity >= 26),
		Range(frame="lo_quantity", lo_quantity <= 35)
	),
frame="lo_revenue_computed", field="lo_revenue_computed")`

func (s *Server) SingleSumRaw(q, qname string) {
	start := time.Now()
	// fmt.Println(q)
	response, err := s.Client.Query(s.Index.RawQuery(q), nil)
	if err != nil {
		fmt.Printf("%v failed with: %v\n", q, err)
		return
	}
	fmt.Printf("%v\n", response.Results()[0].Sum)
	seconds := time.Now().Sub(start).Seconds()
	fmt.Printf("%s: %f sec\n", qname, seconds)
}

// rowID=1 -> year=1993
const q11b = `
Sum(
	Intersect(
		Bitmap(frame="lo_year", rowID=1),
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
frame="lo_revenue_computed", field="lo_revenue_computed")`

// rowID = 24 -> yearmonth=199801
const q12b = `
Sum(
	Intersect(
		Bitmap(frame="lo_year", rowID=6),
		Bitmap(frame="lo_month", rowID=0),
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
frame="lo_revenue_computed", field="lo_revenue_computed")`

// rowID=2 -> year=1994
const q13b = `
Sum(
	Intersect(
		Bitmap(frame="lo_weeknum", rowID=6),
		Bitmap(frame="lo_year", rowID=2),
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
frame="lo_revenue_computed", field="lo_revenue_computed")`

func (s *Server) SingleAggregateRaw(q, qname string) {
	start := time.Now()
	// fmt.Println(q)
	response, err := s.Client.Query(s.Index.RawQuery(q), nil)
	if err != nil {
		fmt.Printf("%v failed with: %v\n", q, err)
		return
	}
	fmt.Printf("%v\n", response.Results()[0].Sum)
	seconds := time.Now().Sub(start).Seconds()
	fmt.Printf("%s: %f sec\n", qname, seconds)

}

func (s *Server) HandleQuery11(w http.ResponseWriter, r *http.Request) {
	s.SingleSumRaw(q11, "query 1.1")
}

func (s *Server) HandleQuery12(w http.ResponseWriter, r *http.Request) {
	s.SingleSumRaw(q12, "query 1.2")
}

func (s *Server) HandleQuery13(w http.ResponseWriter, r *http.Request) {
	s.SingleSumRaw(q13, "query 1.3")
}

func (s *Server) HandleQuery11b(w http.ResponseWriter, r *http.Request) {
	s.SingleAggregateRaw(q11b, "query 1.1b")
}

func (s *Server) HandleQuery12b(w http.ResponseWriter, r *http.Request) {
	s.SingleAggregateRaw(q12b, "query 1.2b")
}

func (s *Server) HandleQuery13b(w http.ResponseWriter, r *http.Request) {
	s.SingleAggregateRaw(q13b, "query 1.3b")
}
