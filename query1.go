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
		Bitmap(frame="d_year", rowID=1),
		Range(frame="bsi", lo_discount >= 1),
		Range(frame="bsi", lo_discount <= 3),
		Range(frame="bsi", lo_quantity < 25)
	),
frame="bsi", field="revenue_computed")`

// rowID = 24 -> yearmonth=199801
const q12 = `
Sum(
	Intersect(
		Bitmap(frame="d_yearmonth_id", rowID=24),
		Range(frame="bsi", lo_discount >= 4),
		Range(frame="bsi", lo_discount <= 6),
		Range(frame="bsi", lo_quantity >= 26),
		Range(frame="bsi", lo_quantity <= 35)
	),
frame="bsi", field="revenue_computed")`

// rowID=2 -> year=1994
const q13 = `
Sum(
	Intersect(
		Bitmap(frame="d_weeknuminyear", rowID=6),
		Bitmap(frame="d_year", rowID=2),
		Range(frame="bsi", lo_discount >= 5),
		Range(frame="bsi", lo_discount <= 7),
		Range(frame="bsi", lo_quantity >= 26),
		Range(frame="bsi", lo_quantity <= 35)
	),
frame="bsi", field="revenue_computed")`

func (s *Server) SingleSumRaw(q, qname string) {
	start := time.Now()
	fmt.Println(q)
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
